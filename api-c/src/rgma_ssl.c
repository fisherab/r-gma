/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This module implements an SSL layer (between rgma_http and rgma_tcp) to
 support HTTPS connections. The OpenSSL implementation comes almost verbatim
 from the ServletConnection and SSL* classes in the C++ API. The alternative
 dummy implementation does nothing except fail any requests to make a secure
 connection: it's just there so we don't need lots of conditional code in
 the API to support implementations that don't have OpenSSL available.

 This file is much too long, but as it's a short-term fix (the gSOAP version
 of the API will not use it) it's probably better to keep it all together. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "rgma_ssl.h"

#define PRIVATE static
#define PUBLIC /* empty */

#include <openssl/ssl.h>      /* for SSL object & SSL_, BIO_, PEM_ functions */
#include <openssl/err.h>      /* for ERR_peek_error function */
#include <openssl/x509v3.h>   /* for X509_ functions */
#include <sys/types.h>        /* see rgmassl_ping */
#include <sys/times.h>        /* see rgmassl_ping */
#include <sys/select.h>       /* for select */
#include <fnmatch.h>          /* for fnmatch function */
#include <unistd.h>           /* see rgmassl_ping */

#include "rgma_lib.h"         /* for lib_getProperty */

#define DEFAULT_CAFILES "/etc/grid-security/certificates"
#define MAX_FILE 255 /* Maximum length of policy file name (full path). */
#define MAX_LINE 255 /* Maximum length of line in policy file. */

enum {
    false = 0, true = 1
};

typedef struct {
    SSL_CTX *ctx; /* Set in get_context and free_context */
    char *sslCAFiles; /* Set in load_trustFile */
    char *gridProxyFile; /* Set in load_trustFile */
    char *sslKeyPassword; /* Set in load_trustFile */
    char *sslCertFile; /* Set in load_trustFile */
    char *sslKey; /* Set in load_trustFile */
} CONTEXT_DETAILS;

typedef struct {
    char *access_id_CA; /* Set in load_policyFile */
    char *pos_rights; /* Set in load_policyFile */
    char *cond_subjects; /* Set in load_policyFile */
    int num_subjects; /* Set in load_policyFile */
} SIGNING_POLICY_DETAILS;
PRIVATE CONTEXT_DETAILS CONTEXT = { NULL, NULL, NULL, NULL, NULL, NULL };
PRIVATE SSL_CTX *get_context(char**);
PRIVATE void free_context(void);
PRIVATE int load_gridProxyFile(SSL_CTX *, const char *);
PRIVATE int getPasswordCallback(char *, int, int, void *);
PRIVATE int load_trustFile(void);
PRIVATE int certVerifyCallback(X509_STORE_CTX *, void *);
PRIVATE int checkIssued(X509_STORE_CTX *, X509 *, X509 *);
PRIVATE int verifyCallback(int, X509_STORE_CTX *);
PRIVATE int checkProxy(X509 *, X509_STORE_CTX *);
PRIVATE int checkCRL(X509 *, X509_STORE_CTX *);
PRIVATE int CRLValid(X509 *, X509_CRL *, X509_STORE_CTX *);
PRIVATE int CertificateNotRevoked(X509 *, X509_CRL *, X509_STORE_CTX *);
PRIVATE int checkSigningPolicy(X509 *, X509_STORE_CTX *);
PRIVATE int load_policyFile(X509 *, char *, SIGNING_POLICY_DETAILS *);
PRIVATE int parse_policy(char *, int);
PRIVATE int isAProxy(X509 *);
//PRIVATE void display_certificate(FILE *, char *, X509 *);


/* PUBLIC FUNCTIONS ***********************************************************/

/**
 * Creates SSL context (i.e. loads credentials) if not already done, then
 * attempts to negotiate an SSL connection on given socket. The OpenSSL code
 * comes verbatim from the ServletConnection class in the C++ API.
 *
 * @param  sock       Open TCP socket.
 *
 * @return Pointer to new SSL object (as RGMA_SSL), or NULL on error.
 */

PUBLIC RGMA_SSL rgmassl_connect(int sock, char** erm) {
    SSL_CTX *ctx;
    SSL *ssl;
    BIO *sbio;

    /* Load SSL context (credentials) if not already done. Note that
     most of this file is concerned with achieving this. */

    ctx = get_context(erm);
    if (ctx == NULL)
        return NULL;

    /* Create new OpenSSL SSL structure for the new connection. */

    ssl = SSL_new(ctx);
    if (ssl == NULL) {
        free_context(); /* not done in C++ API */
        return NULL;
    }

    /* BIO is an OpenSSL I/O abstraction for handling encrypted I/O,
     unencrypted I/O and file I/O. Just think of it as a socket... */

    sbio = BIO_new_socket(sock, BIO_NOCLOSE);
    if (sbio == NULL) {
        SSL_free(ssl); /* not done in C++ API */
        free_context(); /* not done in C++ API */
        return NULL;
    }

    SSL_set_bio(ssl, sbio, sbio);

    /* We can now attempt to make the connection. If it fails, it's
     recommended (in the C++ API) to try it again just once. */

    int sslConnectionStatus = SSL_connect(ssl);
    if (sslConnectionStatus <= 0) {
        int errorCode = SSL_get_error(ssl, sslConnectionStatus);
        char* buff;
        if (errorCode == SSL_ERROR_SSL) {
            char* preMsg = "SSL connect failed: %";
            char* msg = ERR_error_string(ERR_get_error(), NULL);
            buff = (char *) malloc(strlen(preMsg) + strlen(msg) + 1);
            if (buff) {
                *erm = buff;
                strcpy(buff, preMsg);
                strcat(buff, msg);
            }
        } else {
            char* preMsg = "SSL_connect failed with code ";
            char msg[50];
            snprintf(msg, 50, "%d", errorCode);
            buff = (char *) malloc(strlen(preMsg) + strlen(msg) + 1);
            if (buff) {
                *erm = buff;
                strcpy(buff, preMsg);
                strcat(buff, msg);
            }
        }
        SSL_free(ssl);
        free_context();
        return NULL;
    }

    return (RGMA_SSL) ssl;
}

/**
 * Reads bytes from a secure connection.
 *
 * @param  ssl       Open SSL connection.
 * @param  buf       Buffer into which to read bytes.
 * @param  num       Number of bytes to read.
 *
 * @return Number of bytes read or -1 on error.
 */

PUBLIC
int rgmassl_recv(RGMA_SSL ssl, void *buf, int num) {
    return SSL_read((SSL *) ssl, buf, num);
}

/**
 * Writes bytes to a secure connection.
 *
 * @param  ssl       Open SSL connection.
 * @param  buf       Buffer into which to write bytes.
 * @param  num       Number of bytes written.
 *
 * @return Number of bytes written or -1 on error. Note that unlike a
 *         normal socket write(), a return value of zero is also an error.
 */

PUBLIC
int rgmassl_send(RGMA_SSL ssl, const void *buf, int num) {
    return SSL_write((SSL *) ssl, buf, num);
}

/**
 * Shuts down a secure connection (this will send a "close notify" message
 * to the peer, but as we will be closing the underying socket, we do not
 * need to wait for a reply - see the OpenSSL documentation).
 *
 * Note: it seems that Tomcat sometimes sends a "close notify" then shuts the
 * connection without waiting for us to reply. On Linux systems, this results
 * in a SIGPIPE when we attempt to send our "close notify". Short of trapping
 * and discarding that signal, we attempt to avoid this situation by not
 * sending a "close notify" if we've already received one from the server.
 *
 * @param  ssl       Open SSL connection.
 *
 * @return See SSL_shutdown().
 */

PUBLIC
int rgmassl_shutdown(RGMA_SSL ssl) {
    if (SSL_get_shutdown((SSL *) ssl) == 0) {
        return SSL_shutdown((SSL *) ssl);
    }
    return 1;
}

/**
 * Frees resources associated with SSL object.
 *
 * @param  ssl       SSL object.
 *
 * @return Nothing.
 */

PUBLIC
void rgmassl_free(RGMA_SSL ssl) {
    SSL_free((SSL *) ssl);
}

/**
 * Cleans up any static library objects (called by rgma_servlet_stubs).
 *
 * @param  None.
 *
 * @return Nothing.
 */

PUBLIC
void rgmassl_cleanup(void) {
    free_context();
}

/**
 * If an SSL connection times out between HTTP requests, we want to know that
 * the connection is bad BEFORE we send the request. There seems to be no way
 * in OpenSSL to get this information - it's only on the read that we finally
 * get to hear about the "close notify". To get around this, we sniff for
 * pending reads on the connection to which we are about to write, because that
 * indicates some sort of SSL notification (or an HTTP protocol failure, which
 * is unlikely).
 *
 * @param  ssl       Open SSL connection.
 * @param  socket    Open TCP socket.
 *
 * @return 0 if there's nothing pending, 1/2 if there is.
 */

PUBLIC
int rgmassl_ping(RGMA_SSL ssl, int socket) {
    fd_set rfds;
    struct timeval tv;
    char byte;

    tv.tv_sec = 0;
    tv.tv_usec = 0;
    FD_ZERO(&rfds);
    FD_SET(socket, &rfds);
    if (select(socket + 1, &rfds, NULL, NULL, &tv)) {
        SSL_read((SSL *) ssl, &byte, 1); /* dummy read */
        if (SSL_get_shutdown(ssl) == SSL_RECEIVED_SHUTDOWN) {
            return 1; /* server sent "close notify" */
        } else
            return 2; /* HTTP protocol error */
    }
    return 0; /* Nothing to read, SSL connection is OK */
}

/* PRIVATE FUNCTIONS **********************************************************/

/* All of these functions relate to setting up the SSL context - i.e. loading
 and checking the user's credentials: the SSL context is static as all calls
 (and all threads) can use the same context. All of these functions are
 derived from the SSL* classes in the C++ API. In particular, the calls to
 the OpenSSL library and the certificate verification functions are taken
 verbatim from that code. The functions are grouped here as they are in the
 C++ API, and each group is preceded by a comment showing which file they are
 taken from: see the C++ header files for details. */

/* From SSLContext.cpp ********************************************************/

/* Loads user's credentials to create new SSL context. Updates the static
 global CONTEXT - this function, free_context() and load_trustFile() are
 the only places where CONTEXT is written to. */

PRIVATE SSL_CTX *get_context(char** erm) {
    /* Return existing context, if already initialised, otherwise load
     credentials and create a new one. */

    if (CONTEXT.ctx != NULL)
        return CONTEXT.ctx; /* already initialised */

    if (!load_trustFile()) {
        char* msg = "Neither TRUSTFILE nor X509_USER_PROXY environment variable set";
        char* buff = (char *) malloc(strlen(msg) + 1);
        if (buff) {
            *erm = buff;
            strcat(buff, msg);
        }
        free_context();
        return NULL;
    }

    SSL_library_init();
    SSL_load_error_strings();

    CONTEXT.ctx = SSL_CTX_new(TLSv1_client_method());
    if (CONTEXT.ctx == NULL) {
        free_context();
        return NULL;
    }

    if (SSL_CTX_load_verify_locations(CONTEXT.ctx, NULL, CONTEXT.sslCAFiles) != 1) {
        free_context();
        return NULL;
    }

    if (CONTEXT.gridProxyFile != NULL) { /* Use grid proxy file. */
        if (!load_gridProxyFile(CONTEXT.ctx, CONTEXT.gridProxyFile)) {
            char* msg = "Unable to read proxy certificate";
                  char* buff = (char *) malloc(strlen(msg) + 1);
                  if (buff) {
                      *erm = buff;
                      strcat(buff, msg);
                  }
            free_context();
            return NULL;
        }
    } else /* Use certificate + private key. */
    {
        if (CONTEXT.sslKeyPassword != NULL) {
            SSL_CTX_set_default_passwd_cb(CONTEXT.ctx, getPasswordCallback);
            SSL_CTX_set_default_passwd_cb_userdata(CONTEXT.ctx, NULL);
        }
        SSL_CTX_use_certificate_file(CONTEXT.ctx, CONTEXT.sslCertFile, SSL_FILETYPE_PEM);
        SSL_CTX_use_PrivateKey_file(CONTEXT.ctx, CONTEXT.sslKey, SSL_FILETYPE_PEM);
        SSL_CTX_check_private_key(CONTEXT.ctx);
    }

    SSL_CTX_set_verify_depth(CONTEXT.ctx, 5);
    SSL_CTX_set_cert_verify_callback(CONTEXT.ctx, certVerifyCallback, NULL);
    SSL_CTX_set_verify(CONTEXT.ctx, SSL_VERIFY_PEER, verifyCallback);
    SSL_CTX_set_purpose(CONTEXT.ctx, X509_PURPOSE_ANY);

    return CONTEXT.ctx;
}

/* Frees SSL context (so it can be reloaded, if necessary). */

PRIVATE void free_context(void) {
    if (CONTEXT.ctx != NULL)
        SSL_CTX_free(CONTEXT.ctx);
    CONTEXT.ctx = NULL;
    if (CONTEXT.sslCAFiles)
        free(CONTEXT.sslCAFiles);
    CONTEXT.sslCAFiles = NULL;
    if (CONTEXT.gridProxyFile)
        free(CONTEXT.gridProxyFile);
    CONTEXT.gridProxyFile = NULL;
    if (CONTEXT.sslKeyPassword)
        free(CONTEXT.sslKeyPassword);
    CONTEXT.sslKeyPassword = NULL;
    if (CONTEXT.sslCertFile)
        free(CONTEXT.sslCertFile);
    CONTEXT.sslCertFile = NULL;
    if (CONTEXT.sslKey)
        free(CONTEXT.sslKey);
    CONTEXT.sslKey = NULL;
}

/* Loads GRID PROXY FILE. */

PRIVATE int load_gridProxyFile(SSL_CTX *ctx, const char *gridProxyFile) {
    BIO *in;
    X509 *proxyCertificate, *caCertificate;
    EVP_PKEY *privateKey;
    int chainLength;

    in = BIO_new(BIO_s_file_internal());
    if (!in)
        return 0;
    if (BIO_read_filename(in, gridProxyFile) <= 0) {
        BIO_free(in);
        return 0;
    }

    proxyCertificate = PEM_read_bio_X509(in, NULL, ctx->default_passwd_callback, ctx->default_passwd_callback_userdata);
    if (proxyCertificate == NULL) {
        /* BIO_free(in); */
        return 0;
    }

    if (!SSL_CTX_use_certificate(ctx, proxyCertificate)) {
        /* BIO_free(in); */
        /* X509_free(proxyCertificate); */
        return 0;
    }

    /* C++ API displays certificate details at this point... */

    privateKey = PEM_read_bio_PrivateKey(in, NULL, ctx->default_passwd_callback, ctx->default_passwd_callback_userdata);
    if (privateKey == NULL) {
        /* BIO_free(in); */
        /* X509_free(proxyCertificate); */
        return 0;
    }

    if (!SSL_CTX_use_PrivateKey(ctx, privateKey)) {
        /* BIO_free(in); */
        /* X509_free(proxyCertificate); */
        /* free PrivateKey somehow */
        return 0;
    }

    if (ERR_peek_error()) {
        /* BIO_free(in); */
        /* X509_free(proxyCertificate); */
        /* free PrivateKey somehow */
        return 0;
    }

    if (ctx->extra_certs) {
        sk_X509_pop_free(ctx->extra_certs, X509_free);
        ctx->extra_certs = NULL;
    }

    /* Load the entire proxy certificate chain from the grid proxy file.
     Make the assumption that the certificates are in the right order. */

    chainLength = 0;
    while ((caCertificate = PEM_read_bio_X509(in, NULL, ctx->default_passwd_callback,
            ctx->default_passwd_callback_userdata))) {
        ++chainLength;
        /* C++ API displays certificate details at this point... */
        SSL_CTX_add_extra_chain_cert(ctx, caCertificate);
        /* C++ code says don't X509_free(caCertificate)... */
    }

    if (chainLength == 0) /* must have at least one */
    {
        /* BIO_free(in); */
        /* X509_free(proxyCertificate); */
        /* free PrivateKey somehow */
        return 0;
    }

    /* Free up proxy certificate only (see C++ code) */

    X509_free(proxyCertificate);
    BIO_free(in);
    return 1;
}

/* Password callback (user is prompted at command line if this is not used). */

PRIVATE int getPasswordCallback(char *buffer, int size, int rwflag, void *unused) {
    strncpy(buffer, CONTEXT.sslKeyPassword, size);
    buffer[size - 1] = '\0';
    return strlen(buffer);
}

/* From SSLContextProperties.cpp **********************************************/

/* Load SSL context properties from TRUSTFILE. Fills in most of the fields in
 the CONTEXT global. */

PRIVATE int load_trustFile(void) {
    char *trustFile, *gridProxyFile, *sslCAFiles, *s;

    /* Set defaults. */

    CONTEXT.sslCAFiles = NULL;
    CONTEXT.gridProxyFile = NULL;
    CONTEXT.sslCertFile = NULL;
    CONTEXT.sslKey = NULL;
    CONTEXT.sslKeyPassword = NULL; /* can remain NULL */

    /* Now override them. */

    if ((gridProxyFile = getenv("X509_USER_PROXY"))) {
        CONTEXT.gridProxyFile = (char *) malloc(strlen(gridProxyFile) + 1);
        if (CONTEXT.gridProxyFile == NULL)
            return false;
        strcpy(CONTEXT.gridProxyFile, gridProxyFile);
        if ((sslCAFiles = getenv("X509_CERT_DIR"))) {
            CONTEXT.sslCAFiles = (char *) malloc(strlen(sslCAFiles) + 1);
            if (CONTEXT.sslCAFiles == NULL)
                return false;
            strcpy(CONTEXT.sslCAFiles, sslCAFiles);
        }
    } else if ((trustFile = getenv("TRUSTFILE"))) {
        CONTEXT.sslCAFiles = lib_getProperty(trustFile, "sslCAFiles");
        CONTEXT.sslKey = lib_getProperty(trustFile, "sslKey");
        CONTEXT.sslCertFile = lib_getProperty(trustFile, "sslCertFile");
        CONTEXT.sslKeyPassword = lib_getProperty(trustFile, "sslKeyPassword");

        if ((CONTEXT.sslCertFile == NULL) || (CONTEXT.sslKey == NULL)) {
            CONTEXT.gridProxyFile = lib_getProperty(trustFile, "gridProxyFile");
        }

        /* Allow this API to work with (broken) Java API trustfiles
         i.e. allow wildcard certificate names on "sslCAFiles" (which
         should really just be a directory) and read the password
         from "sslKeyPasswd" (it should be sslKeyPassword). */

        if (CONTEXT.sslCAFiles) {
            if ((s = strstr(CONTEXT.sslCAFiles, "*.")))
                *s = '\0';
        }
        if (!CONTEXT.gridProxyFile && !CONTEXT.sslKeyPassword) {
            CONTEXT.sslKeyPassword = lib_getProperty(trustFile, "sslKeyPasswd");
        }
    }

    if (CONTEXT.gridProxyFile == NULL && (CONTEXT.sslKey == NULL || CONTEXT.sslCertFile == NULL)) {
        return false; /* Must have one or the other. */
    }

    if (CONTEXT.sslCAFiles == NULL) /* use default if not set */
    {
        CONTEXT.sslCAFiles = (char *) malloc(strlen(DEFAULT_CAFILES) + 1);
        if (CONTEXT.sslCAFiles)
            strcpy(CONTEXT.sslCAFiles, DEFAULT_CAFILES);
    }

    return true;
}

/* From SSLCertVerifyCallback.cpp *********************************************/

/* Certificate verification callback. */

PRIVATE int certVerifyCallback(X509_STORE_CTX *X509StoreContext, void *argument) {
    X509StoreContext->check_issued = checkIssued; /* function pointer */
    return (X509_verify_cert(X509StoreContext));
}
PRIVATE int checkIssued(X509_STORE_CTX *X509StoreContext, X509 *certificate, X509 *issuer) {
    int issuedStatus;

    issuedStatus = X509_check_issued(issuer, certificate);
    if (issuedStatus == X509_V_OK || (issuedStatus == X509_V_ERR_KEYUSAGE_NO_CERTSIGN && isAProxy(certificate))) {
        return true;
    }

/* Set up error details for issuer errors if requested. */
#if OPENSSL_VERSION_NUMBER < 0x00908000
    if (!(X509StoreContext->flags & X509_V_FLAG_CB_ISSUER_CHECK)) {
#else
    if (!(X509StoreContext->param->flags & X509_V_FLAG_CB_ISSUER_CHECK)) {
#endif
        X509StoreContext->error = issuedStatus;
        X509StoreContext->current_cert = certificate;
        X509StoreContext->current_issuer = issuer;
        if (X509StoreContext->verify_cb) {
            return X509StoreContext->verify_cb(0, X509StoreContext);
        }
    }

    return false;
}

/* From SSLVerifyCallback.cpp *************************************************/

/* Certificate verification callback. */

PRIVATE int verifyCallback(int ok, X509_STORE_CTX *X509StoreContext) {
    X509 *currentCert;

    currentCert = X509_STORE_CTX_get_current_cert(X509StoreContext);
    /* C++ code displays certificate details here */

    if (ok) {
        if (isAProxy(currentCert)) {
            ok = checkProxy(currentCert, X509StoreContext);
        } else /* non-proxy checks */
        {
            ok = (checkCRL(currentCert, X509StoreContext) && checkSigningPolicy(currentCert, X509StoreContext));
        }
    }

    return ok;
}

/* Checks proxy certificate is permitted. */

PRIVATE int checkProxy(X509 *certificate, X509_STORE_CTX *X509StoreContext) {
    char *subject, *issuer, *proxyString;
    int ok, length, depth;

    ok = true;

    /* Check that the user who issued the certificate signed the proxy. */

    subject = X509_NAME_oneline(X509_get_subject_name(certificate), NULL, 0);
    issuer = X509_NAME_oneline(X509_get_issuer_name(certificate), NULL, 0);
    proxyString = strstr(subject, "/CN=");
    if (proxyString != NULL) {
        proxyString = strstr(proxyString + 4, "/CN=");
        length = proxyString - subject;
        if (strncmp(subject, issuer, length) != 0) {
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_SUBJECT_ISSUER_MISMATCH);
            ok = false;
        }
    }

    /* Check that the proxy is at the top of the chain.  For delegation
     etc., the proxy might not be at the top of the chain but RGMA does
     not support this at present. */

    if (ok) {
        depth = X509_STORE_CTX_get_error_depth(X509StoreContext);
        if (depth != 0) {
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_APPLICATION_VERIFICATION);
            ok = false;
        }
    }

    OPENSSL_free(subject);
    subject = 0;
    OPENSSL_free(issuer);
    issuer = 0;

    return ok;
}

/* Attempts to check a certificate against the Certicicate Revocation List.
 The C++ code says this check can be removed if using OpenSSL 0.9.7 or
 later. */

PRIVATE int checkCRL(X509 *certificate, X509_STORE_CTX *X509StoreContext) {
    X509_OBJECT X509Object;
    X509_CRL *crl;
    int ok;

    /* If Certificate Revokation List exists, check it. */

    if (X509_STORE_get_by_subject(X509StoreContext, X509_LU_CRL, X509_get_issuer_name(certificate), &X509Object)) {
        crl = X509Object.data.crl;

        ok
                = (CRLValid(certificate, crl, X509StoreContext) && CertificateNotRevoked(certificate, crl,
                        X509StoreContext));
        X509_OBJECT_free_contents(&X509Object);
    } else
        ok = true; /* nothing to check */

    return ok;
}

/* Checks the Certificate Revokation List itself is valid. */

PRIVATE int CRLValid(X509 *certificate, X509_CRL *crl, X509_STORE_CTX *X509StoreContext) {
    X509_CRL_INFO *crlInfo;
    X509 *issuer;
    EVP_PKEY *issuerKey;
    int crlVerify, timeComparison;

    crlInfo = crl->crl;

    /* Check that the CRL is signed by the certificate issuer. */

    if (X509StoreContext->get_issuer(&issuer, X509StoreContext, certificate) <= 0) {
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_SIGNATURE_FAILURE);
        return false;
    }

    issuerKey = X509_get_pubkey(issuer);
    X509_free(issuer);
    issuer = 0;

    if (!issuerKey) {
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_SIGNATURE_FAILURE);
        return false;
    }

    crlVerify = X509_CRL_verify(crl, issuerKey);
    EVP_PKEY_free(issuerKey);
    issuerKey = 0;

    if (crlVerify <= 0) {
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_SIGNATURE_FAILURE);
        return false;
    }

    /* Check CRL (time) validity. */

    timeComparison = X509_cmp_current_time(crlInfo->lastUpdate);
    if (timeComparison == 0) {
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD);
        return false;
    } else if (timeComparison > 0) {
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_NOT_YET_VALID);
        return false;
    }

    if (crlInfo->nextUpdate) {
        timeComparison = X509_cmp_current_time(crlInfo->nextUpdate);
        if (timeComparison == 0) {
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD);
            return 0;
        } else if (timeComparison < 0) {
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_HAS_EXPIRED);
            /* shouldn't we return FALSE? */
        }
    }

    return true; /* It's OK */
}

/* Checks a certificate is not revoked. */

PRIVATE int CertificateNotRevoked(X509 *certificate, X509_CRL *crl, X509_STORE_CTX *X509StoreContext) {
    X509_CRL_INFO *crlInfo;
    X509_REVOKED *revoked;
    int numCertificates;
    int i;

    /*  The ASN1_INTEGER_cmp function here comes from OpenSSL. */

    crlInfo = crl->crl;
    numCertificates = sk_X509_REVOKED_num(crlInfo->revoked);
    for (i = 0; i < numCertificates; ++i) {
        revoked = (X509_REVOKED *) sk_X509_REVOKED_value(crlInfo->revoked, i);
        if (!ASN1_INTEGER_cmp(revoked->serialNumber, X509_get_serialNumber(certificate))) {
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CERT_REVOKED);
            return false;
        }
    }

    return true; /* it's OK */
}

/* Checks the signing policy for a certificate. */

PRIVATE int checkSigningPolicy(X509 *certificate, X509_STORE_CTX *X509StoreContext) {
    X509_NAME *issuerName, *subjectName;
    SIGNING_POLICY_DETAILS policy;
    char *issuer, *subject, *pattern;
    const char *p, *s, *t;
    int ok, i;

    issuerName = X509_get_issuer_name(certificate);
    subjectName = X509_get_subject_name(certificate);
    if (X509_NAME_cmp(subjectName, issuerName) == 0) {
        return true; /* self-signed (no need to check) */
    }

    /* Load policies from file (we have to depart from the C++ API
     for the remainder of this function as we need to convert it to
     pure C). */

    if (!load_policyFile(certificate, CONTEXT.sslCAFiles, &policy) || policy.access_id_CA == NULL || policy.pos_rights
            == NULL || policy.cond_subjects == NULL) {
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_APPLICATION_VERIFICATION);
        return false;
    }

    /* Now run policy checks. */

    issuer = X509_NAME_oneline(issuerName, NULL, 0);
    subject = X509_NAME_oneline(subjectName, NULL, 0);
    ok = true; /* default */

    /* Check access id matches issuer. The e-mail hack here allows both
     alternative spellings of the e-mail keyword to be handled. This
     code is deliberately identical to the C++ version. */

    if (ok && strcmp(policy.access_id_CA, issuer) != 0) {
        p = policy.access_id_CA, issuer; // short-hand

        if ((((s = strstr(p, "/Email=")) != NULL) && ((t = strstr(issuer, "/emailAddress=")) != NULL) && (strncmp(p,
                issuer, (s - p)) == 0) && // match 1st half
                (strcmp((s + 7), (t + 14)) == 0)) || // match 2nd half
                (((s = strstr(p, "/emailAddress=")) != NULL) && ((t = strstr(issuer, "/Email=")) != NULL) && (strncmp(
                        p, issuer, (s - p)) == 0) && // match 1st half
                        (strcmp((s + 14), (t + 7)) == 0))) // match 2nd half
        {
            ok = true; // so far, so good
        } else {
            ok = false;
        }
    }

    /* Check that CA has signing rights. */

    if (ok && strcmp(policy.pos_rights, "CA:sign") != 0) {
        ok = false;
    }

    /* Check that the subject is one which the CA can sign. The fnmatch()
     function here is Unix-specific unfortunately (it does a string
     match with a "shell wildcard pattern" (see fnmatch(3)). */

    if (ok) {
        pattern = policy.cond_subjects;
        for (i = 0; i < policy.num_subjects; ++i) {
            if (fnmatch(pattern, subject, FNM_NOESCAPE) == 0) {
                break;
            }
            pattern += strlen(pattern) + 1;
        }

        if (i == policy.num_subjects)
            ok = false; /* not found */
    }

    OPENSSL_free(subject);
    subject = 0; /* for security */
    OPENSSL_free(issuer);
    issuer = 0; /* for security */

    if (!ok)
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_APPLICATION_VERIFICATION);

    if (policy.access_id_CA)
        free(policy.access_id_CA);
    if (policy.pos_rights)
        free(policy.pos_rights);
    if (policy.cond_subjects)
        free(policy.cond_subjects);
    return ok;
}

/* From SSLSigningPolicyProperties.cpp ****************************************/

/* Loads "policy" from the Signing Policy file. */

PRIVATE int load_policyFile(X509 *certificate, char *directory, SIGNING_POLICY_DETAILS *policy) {
    FILE *fp;
    int num_fields;
    char filename[MAX_FILE], line[MAX_LINE], *type, *authority, *value;

    /* Initialise defaults. */

    policy->access_id_CA = NULL;
    policy->pos_rights = NULL;
    policy->cond_subjects = NULL;
    policy->num_subjects = 0;

    /* Construct policy file name and open file. */

    sprintf(filename, "%s/%08lx.signing_policy", directory, X509_NAME_hash(X509_get_issuer_name(certificate)));
    fp = fopen(filename, "r");
    if (fp == NULL)
        return false;

    while (fgets(line, sizeof(line), fp)) {
        /* Check we got a complete line. */

        if (!strchr(line, '\n')) {
            fclose(fp);
            return false;
        }

        /* Parse line into its three fields. */

        num_fields = parse_policy(line, '\'');
        if (num_fields != 3)
            continue;
        type = line;
        authority = type + strlen(type) + 1; /* not used */
        value = authority + strlen(authority) + 1;

        /* Extract values for the interesting properties. */

        if (!policy->access_id_CA && strcmp(type, "access_id_CA") == 0) {
            policy->access_id_CA = (char *) malloc(strlen(value) + 1);
            if (policy->access_id_CA)
                strcpy(policy->access_id_CA, value);
        } else if (!policy->pos_rights && strcmp(type, "pos_rights") == 0) {
            policy->pos_rights = (char *) malloc(strlen(value) + 1);
            if (policy->pos_rights)
                strcpy(policy->pos_rights, value);
        } else if (!policy->cond_subjects && strcmp(type, "cond_subjects") == 0) {
            policy->cond_subjects = (char *) malloc(strlen(value) + 1);
            if (policy->cond_subjects) {
                strcpy(policy->cond_subjects, value);
                policy->num_subjects = parse_policy(policy->cond_subjects, '\"');
            }
        }
    }

    if (ferror(fp)) {
        fclose(fp);
        return false;
    }

    fclose(fp);
    return true;
}

/* Parses a line from the Signing Policy file. This is a text file containing
 a series of one-line entries with each line containing three fields. Field
 values with embedded spaces are enclosed in single quotes. The first field
 defines the record type, the second is the authority and the third is
 the value. For the "cond_subjects" record, the value is itself a
 space-separated sequence of regular expressions, with each regular
 expression enclosed in double quotes. Comments are preceded by #.

 This function replaces C++ code in SSLSigningPolicyProperties.cpp. */

PRIVATE int parse_policy(char *line, int quote_char) {
    int in_space, in_quotes, num_fields;
    char *s, *t;

    in_space = true;
    in_quotes = false;
    num_fields = 0;
    for (s = line, t = line; *s != '\0'; ++s) {
        if (*s == '#') /* comment */
        {
            break;
        } else if (isspace(*s)) /* whitespace */
        {
            if (in_quotes)
                *t++ = *s;
            else {
                if (!in_space)
                    *t++ = '\0';
                in_space = true;
            }
        } else /* anything else */
        {
            if (in_space)
                ++num_fields;
            in_space = false;
            if (*s == quote_char)
                in_quotes = !in_quotes;
            else
                *t++ = *s;
        }
    }

    *t = '\0';
    return num_fields;
}

/* From SSLUtils.cpp **********************************************************/

/* Checks whether certificate is a proxy certificate. */

PRIVATE int isAProxy(X509 *certificate) {
    char *subject, *s;

    /* Look for ".../CN=.../CN=proxy..." or
     ".../CN=.../CN=limited proxy..." or
     ".../CN=.../CN=restricted proxy..." */

    subject = X509_NAME_oneline(X509_get_subject_name(certificate), NULL, 0);
    s = subject;
    if (s)
        s = strstr(s, "/CN=");
    if (s)
        s = strstr(s + 4, "/CN=");
    if (s)
        s += 4;
    if (subject)
        OPENSSL_free(subject);

    return (s != NULL && (strcmp(s, "proxy") == 0 || strcmp(s, "limited proxy") == 0 || strcmp(s, "restricted proxy")
            == 0));
}
