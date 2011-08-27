/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef INFO_SSLCONTEXT_H
#define INFO_SSLCONTEXT_H

#include "rgma/RGMAPermanentException.h"
#include <string>

// OpenSSL types
typedef struct ssl_ctx_st SSL_CTX;

namespace glite {
namespace rgma {

class SSLContextProperties;

/**
 * Secure socket context.  It is encapsulated into ServletConnection as a static
 * object so that it only gets evaluated on the first call to SSLContext's get()
 * method.
 *
 * The context is the certificates and passwords needed to establish the SSL
 * session.
 */

class SSLContext {
    private:
        //static const class APILogging& cat;

        /**
         * This is the context which this class encapuslates.
         */
        SSL_CTX *context;

        /**
         * Password used if the private key's password is stored in the properties file.
         */
        static std::string passwordStatic;

        /*
         * Location of SSL CA files
         */
        static std::string sslCAFilesStatic;

    public:

        /**
         * Constructor does very little - setting up the context is done by get method.
         */
        SSLContext();

        virtual ~SSLContext();

        /**
         * Delete context so that next call to get will re-evaluate it by rereading the
         * certificates etc.  Used to cause the context to be refreshed from file.
         */
        void flush();

        /**
         * Returns pointer to SSL CTX object.  Lazy evaluation so that it is created
         * only when this method is called for the first time (or after a SSLContext::flush()).
         */
        SSL_CTX* get() throw(RGMAPermanentException);

        /**
         * Getter for sslCAFilesStatic static attribute.
         */
        static const std::string& getSslCAFiles();

    private:
        /* OpenSSL callback functions - called from OpenSSL */

        /**
         * Password and OpenSSL callback function to get it are used if the private
         * key's password is stored in the properties file.
         */
        static int getPasswordCallback(char *buf, int size, int rwflag, void *unused);

        /**
         * Builds the certificate chain of the proxy certificate and the users (or service)
         * key.
         */
        void useClientProxyCertificateFile(SSL_CTX *context, std::string file) throw (RGMAPermanentException);

        /**
         * Builds the certificate chain of the normal certificate file and key.
         */
        void useClientCertificateAndKeyFiles(SSL_CTX *context, SSLContextProperties &props);

};
}
}
#endif                                            // EDG_INFO_SSLCONTEXT_H
