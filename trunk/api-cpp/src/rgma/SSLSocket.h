/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMA_SSLSOCKET_H
#define RGMA_SSLSOCKET_H
#define OPENSSL_NO_KRB5

#include "rgma/TCPSocket.h"
#include "rgma/SSLContext.h"

#include <openssl/ssl.h>

namespace glite {
namespace rgma {

/**
 * SSL socket wrapper
 */
class SSLSocket: public TCPSocket {

    public:
         ~SSLSocket();
         SSLSocket();
         void connect(std::string host, int port) throw(RGMATemporaryException, RGMAPermanentException);
         void write(std::string request, std::string& resonse) throw(RGMATemporaryException, RGMAPermanentException);
         void close();

    private:
        SSL* m_ssl;
        SSL_SESSION* m_sslSession;
        std::string m_host;
        int connectionCounter;
        const static int MAX_NO_OF_CONNECTIONS;
        int m_port;
        static SSLContext m_sslContext;
        void establishSSL() throw(RGMATemporaryException, RGMAPermanentException);
        /**
         * attempt to read from an established ssl conection
         * returns 0 if ok, 1 if close notify and
         *  2 if there is something else
         */
        int sslPing(SSL *ssl, int socket);
        /**
         * shut down the ssl connection and tcp connection remake the TCP connection
         * re-establish the ssl connection
         *
         */
        void reMakeSSLConnection() throw(RGMATemporaryException, RGMAPermanentException);

        void testRead(int returnCode) throw(RGMATemporaryException, RGMAPermanentException);


};
}
}
#endif
