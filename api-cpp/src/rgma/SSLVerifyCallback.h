/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef GLITE_SSLVERIFYCALLBACK_H
#define GLITE_SSLVERIFYCALLBACK_H
#define OPENSSL_NO_KRB5

// OpenSSL types
typedef struct X509_crl_st X509_CRL;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct x509_st X509;
typedef struct x509_store_ctx_st X509_STORE_CTX;

namespace glite
{
    namespace rgma
    {
        /**
         * SSL verify callback
         *
         */
        class SSLVerifyCallback
        {

            /**
             * Static class contains functions for verifying SSL certificates from the
             * server.  Entry point is main(), which is called by OpenSSL.
             */

            public:

                /**
                 * OpenSSL callback called after each certificate has been verified.
                 * Displays diagnostic information if debugging is enabled and carries out
                 * some verification.
                 */
                static int main(int preverifyOk, X509_STORE_CTX *X509Context);

            private:
                // Helper functions for certificateVerifyCallback

                /*
                 * Return true if extra checks on proxy certificate pass, false if certificate
                 * not valid.
                 */
                static bool proxyCertificateOk(X509* certificate,
                    X509_STORE_CTX *X509Context);

                /*
                 * Return true if the certificate has not been revoked.
                 */
                static bool CRLCheckOk(X509* certificate,
                    X509_STORE_CTX *X509StoreContext);

                // CRLCheckOk uses the following helper functions:

                /*
                 * Return true if the specified CRL looks OK.
                 */
                static bool CRLValid(X509* certificate,
                    X509_CRL* crl,
                    X509_STORE_CTX *X509StoreContext);
                /*
                 * Return true if the certificate is not listed on the CRL.
                 */
                static bool CertificateNotRevoked(X509* certificate,
                    X509_CRL* crl,
                    X509_STORE_CTX *X509StoreContext);

                /*
                 * Return true if the signing policy on the certificate is OK for RGMA's
                 * purposes.
                 */
                static bool signingPolicyCheckOk(X509* certificate,
                    X509_STORE_CTX *X509StoreContext);

            private:
                //static const class APILogging& cat;

        };
    }
}
#endif                                            // GLITE_SSLVERIFYCALLBACK_H
