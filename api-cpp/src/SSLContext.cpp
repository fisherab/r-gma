/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

///
// Secure socket context.  It is encapsulated into ServletConnection as a static
// object so that it only gets evaluated on the first call to SSLContext's get()
// method.
//
// The context is the certificates and passwords needed to establish the SSL
// session.
//
// ServletConnection uses OpenSSL to create an SSL connection between the client
// (i.e. the code running the C++ API) and the server running the servlets.  The
// OpenSSL context is created by lazy evaluation by the class SSLContext.  The context
// is created when the first SSL connection is opened and kept thereafter.
//
// The SSL context structure (which is encapsulated by SSLContext) defines some
// callbacks which the code needs to use.  Two of these are used - the code for them
// is contained in static C++ classes.
//
// The properties for SSLContext - files to use etc - are handled by the
// SSLContextProperties class.  It encapsulates rather than inherits from the Properties
// because the interface is much more property-specific than that of Properties.
//
// One of these is the certificate verify chain callback called to verify the
// certificate chain that has been send by the server.  The code for this is contained
// in the static object SSLCertVerifyCallback.  All that this does is replace the
// code for checking if one certificate is the issuer for another.  That piece of
// code is contained (as a pointer to a function) in the certificate store object
// passed in to the certificate verify chain callback.  Once it has done this it just
// calls the default OpenSSL one.
//
// The other is the verify callback called after each server certificate has been
// verified.  It is contained in SSLVerifyCallback object.  The code here is quite
// long as additional checks have to be done for proxy certificates, for Certificate
// Revocation Lists and for the Certificate Authority's signing policy.
//
// The signing policy is contained in a text file in the Grid Security certificates area,
// and this is parsed and encapsulated by a SSLSigningPolicyProperties object.  Signing
// Policies aren't part of SSL, but the prefix is on the class to give a hint that it is
// associated with SSL. This class inherits from the Properties class, as it is a
// specialised type of properties, and uses some of the functions in the super-class.
//
// Finally, SSLUtils contains some utility static methods that don't really fit anywhere
// in particular.
//
// ServletConnection uses SSLContext
// SSLContext uses SSLContextProperties, SSLCertVerifyCallback, SSLVerifyCallback and
//                   SSLUtils
// SSLCertVerifyCallback uses SSLUtils
// SSLVerifyCallback uses SSLSigningPolicyProperties and SSLUtils
//

#include "rgma/SSLCertVerifyCallback.h"
#include "rgma/SSLContext.h"
#include "rgma/SSLContextProperties.h"
#include "rgma/SSLUtils.h"
#include "rgma/SSLVerifyCallback.h"
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#if RGMA_OPENSSL_CERT_VERIFY_HACK == 1
extern "C" int SSLCertVerifyCallbackFunction();
#define SSL_CERT_VERIFY_CALLBACK_FUNCTION &SSLCertVerifyCallbackFunction
#else
#define SSL_CERT_VERIFY_CALLBACK_FUNCTION &SSLCertVerifyCallback::main
#endif

namespace glite {
namespace rgma {

// Static object ensures that SSL_CTX free function is called on program
// exit
SSLContext::SSLContext() :
    context(0) {
}

// Drop context if it has been evaluated
void SSLContext::flush() {
    sslCAFilesStatic = "";
    passwordStatic = "";
    if (context) {
        SSL_CTX_free(context);
        context = 0;
    }
}

SSLContext::~SSLContext() {
    flush();
}

// Lazy evaluation of SSL_CTX
SSL_CTX* SSLContext::get() throw (RGMAPermanentException) {
    if (!context) {
        SSLContextProperties props;
        SSL_library_init();
        SSL_load_error_strings();
        SSL_METHOD* method = TLSv1_client_method();

        context = SSL_CTX_new(method);
        if (!context) {
            throw RGMAPermanentException("Unable to create SSL CTX object");
        }

        // Load the location of the CA certificates used to verify server
        sslCAFilesStatic = props.sslCAFiles();
        if (SSL_CTX_load_verify_locations(context, NULL, sslCAFilesStatic.c_str()) != 1) {
            SSL_CTX_free(context);
            throw RGMAPermanentException("Problem with CA certificates " + sslCAFilesStatic);
        }

        if (props.useCertificateAndKey())
            useClientCertificateAndKeyFiles(context, props);
        else
            useClientProxyCertificateFile(context, props.gridProxyFile());

        SSL_CTX_set_verify_depth(context, 5);

        SSL_CTX_set_cert_verify_callback(context, SSL_CERT_VERIFY_CALLBACK_FUNCTION, NULL);
        SSL_CTX_set_verify(context, SSL_VERIFY_PEER, &SSLVerifyCallback::main);

        // Accept any purpose
        SSL_CTX_set_purpose(context, X509_PURPOSE_ANY);

    }
    return context;
}

void SSLContext::useClientCertificateAndKeyFiles(SSL_CTX *context, SSLContextProperties &props) {
    // Set up the callback for returning the password if it has been specified in the
    // properties file. Otherwise the program will ask for the password at the terminal
    if (props.sslKeyPasswordPresent(passwordStatic)) {
        SSL_CTX_set_default_passwd_cb(context, getPasswordCallback);
        SSL_CTX_set_default_passwd_cb_userdata(context, NULL);
    }

    SSL_CTX_use_certificate_file(context, props.sslCertFile().c_str(), SSL_FILETYPE_PEM);
    SSL_CTX_use_PrivateKey_file(context, props.sslKey().c_str(), SSL_FILETYPE_PEM);
    SSL_CTX_check_private_key(context);
}

void SSLContext::useClientProxyCertificateFile(SSL_CTX *context, std::string filename) throw (RGMAPermanentException) {
    //cat.debug("Entered useClientProxyCertificateFile method");
    BIO *in = BIO_new(BIO_s_file_internal());
    if (!in) {
        throw RGMAPermanentException(std::string("Unable to open proxy certificate file ") + filename);
    }

    if (BIO_read_filename(in,filename.c_str()) <= 0) {
        BIO_free(in);
        throw RGMAPermanentException(std::string("Unable to read proxy certificate file ") + filename);
    }

    // Read proxy certificate.
    X509* proxyCertificate = PEM_read_bio_X509(in, NULL, context->default_passwd_callback,
            context->default_passwd_callback_userdata);
    if (!proxyCertificate) {
        throw RGMAPermanentException("Unable to read proxy certificate");
    }

    if (!SSL_CTX_use_certificate(context, proxyCertificate)) {
        throw RGMAPermanentException("Unable to use proxy certificate");
    }

    // Read private key.
    EVP_PKEY *privateKey = PEM_read_bio_PrivateKey(in, NULL, context->default_passwd_callback,
            context->default_passwd_callback_userdata);
    if (!privateKey) {
        throw RGMAPermanentException("Unable to read private key");
    }

    if (!SSL_CTX_use_PrivateKey(context, privateKey)) {
        throw RGMAPermanentException("Unable to use private key");
    }

    if (ERR_peek_error()) {
        throw RGMAPermanentException("Key/certificate mismatch");
    }

    // Make sure there are no extra certificates already
    if (context->extra_certs) {
        sk_X509_pop_free(context->extra_certs, X509_free);
        context->extra_certs = NULL;
    }

    X509 *chainCertificate = NULL;
    int chain_length = 0;

    // Read certificate chain
    while ((chainCertificate = PEM_read_bio_X509(in, NULL, context->default_passwd_callback,
            context->default_passwd_callback_userdata)) != NULL) {
        SSL_CTX_add_extra_chain_cert(context, chainCertificate);
        chain_length++;
    }

    // Check chain length
    if (chain_length < 1) {
        throw RGMAPermanentException("Failed to read a certificate chain for proxy");
    }

    // Free main certificate - i.e. reduce its use count back to 1
    // CA certificate has its use count at 1 anyway so no need to free it
    //
    // FIXME: Assume the above means we don't need to free any of the chain certificates???

    X509_free(proxyCertificate);
    BIO_free(in);
}

// Callbacks called from OpenSSL

int SSLContext::getPasswordCallback(char *buffer, int size, int rwflag, void *unused) {
    strncpy(buffer, passwordStatic.c_str(), size);
    buffer[size - 1] = '\0';
    return strlen(buffer);
}

const std::string& SSLContext::getSslCAFiles() {
    return sslCAFilesStatic;
}

std::string SSLContext::sslCAFilesStatic = "";
std::string SSLContext::passwordStatic = "";

}
}
