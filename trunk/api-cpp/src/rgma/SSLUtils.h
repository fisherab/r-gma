/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef GLITE_SSLUTILS_H
#define GLITE_SSLUTILS_H
#define OPENSSL_NO_KRB5
// OpenSSL types
typedef struct ssl_ctx_st SSL_CTX;
typedef struct x509_st X509;

namespace glite {
namespace rgma {

/**
 * Static utility functions used by SSL classes, which don't conveniently fit
 * into any particular class.
 */

class SSLUtils {

    public:
        /**
         * Returns true if the certificate is a proxy certificate.
         *
         * @param certificate the certificate to check.
         * @return true if the certificate is a proxy.
         */
        static bool isAProxyCertificate(X509* certificate);
};
}
}
#endif
