/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef INFO_SSLCERTVERIFYCALLBACK_H
#define INFO_SSLCERTVERIFYCALLBACK_H
#define OPENSSL_NO_KRB5
/**
 * OpenSSL 0.9.6 SSL_CTX_set_cert_verify_callback declares its second
 * parameter as int (*)() instead of int (*) (X509_STORE_CTX *, void *).
 * OK for C, but not for C++.  So, make SSL_CTX_set_cert_verify_callback()
 * parameter a function which is declared to take no parameters, but in
 * reality takes the required ones.  It in turn calls
 * SSLCertVerifyCallback::main.
 *
 * OpenSSL 0.9.7 (and hopefully onwards) SSL_CTX_set_cert_verify_callback
 * declares its second parameter int (*) (X509_STORE_CTX *, void *), so use
 * SSLCertVerifyCallback::main directly.
 *
 * This macro controls the behaviour.
 */

#include <openssl/opensslv.h>

#if OPENSSL_VERSION_NUMBER < 0x0090700fL
#define RGMA_OPENSSL_CERT_VERIFY_HACK 1
#else
#define RGMA_OPENSSL_CERT_VERIFY_HACK 0
#endif

// OpenSSL types
typedef struct x509_store_ctx_st X509_STORE_CTX;
typedef struct x509_st X509;

namespace glite
{
    namespace rgma
    {

        /**
         * Encapuslates OpenSSL Callback function to verify certificate chain.
         * Modifies X509 certificate store context to replace the function for
         * finding out of one certificate issued another with one that will accept
         * proxy certificates being signed by certificates which have the X509v3
         * Key Usage field present but where the Key Usage field doesn't specify
         * CA_sign.
         */

        class SSLCertVerifyCallback
        {
            public:

                /**
                 * Callback function.  Replace X509StoreContext->check_issued and
                 * then call the default OpenSSL X509_verify_cert.  Signature is as
                 *required for OpenSSL 0.9.7 SSL_CTX_set_cert_verify_callback().
                 */
                static int main(X509_STORE_CTX* X509StoreContext,
                    void* argument);

                /**
                 * Replacement to the OpenSSL check_issued function pointed to by
                 * X509_STORE_CTX.check_issued. It recognises issuer certificates which
                 * have the X509v3 Key Usage extension field present but without the
                 * CA_Sign authorisation specified in it, when the issuer has signed a
                 * proxy certificate.
                 *
                 * The default OpenSSL implementation won't recognise an issuer which
                 * has the Key Usage extension field present but doesn't have the CA Sign
                 * bit set in it.
                 *
                 * Both versions recognise issuers which don't have the Key Usage extension
                 * field present at all.
                 *
                 * @return 1 if the candidateIssuerCertificate issued certificate, or 0
                 * if it didn't.
                 */
                static int checkIssued(X509_STORE_CTX* X509StoreContext,
                    X509* certificate,
                    X509* candidateIssuerCertificate);

        };
    }
}
#endif                                            // EDG_INFO_SSLCERTVERIFYCALLBACK_H
