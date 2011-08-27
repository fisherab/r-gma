#include "rgma/TCPSocket.h"
#include "rgma/SSLSocket.h"
#include "rgma/RGMAException.h"
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <sstream>
#include <ctype.h>
#include <stdlib.h>
#include <errno.h>

namespace glite {
namespace rgma {
const int SSLSocket::MAX_NO_OF_CONNECTIONS = 80;

SSLSocket::~SSLSocket() {
    if (m_ssl != NULL) {
        SSL_shutdown(m_ssl);
        SSL_free(m_ssl);
    }
}

SSLSocket::SSLSocket() {
    m_ssl = NULL;
    connectionCounter = 0;
}

void SSLSocket::connect(std::string host, int port) throw (RGMATemporaryException, RGMAPermanentException) {
    m_host = host;
    m_port = port;
    TCPSocket::connect(m_host, m_port);
    establishSSL();

}

void SSLSocket::establishSSL() throw (RGMATemporaryException, RGMAPermanentException) {
    connectionCounter = 0;
    bool failedOnce = false;
    int sslConnectionStatus = 0;
    do {
        if (m_ssl == NULL) {
            m_ssl = SSL_new(m_sslContext.get());
            if (!m_ssl) {
                throw RGMAPermanentException("Function unexpectedly returned a null pointer");
            }

            BIO* sbio = BIO_new_socket(m_sock, BIO_NOCLOSE);
            if (!sbio) {
                throw RGMAPermanentException("Function unexpectedly returned a null pointer");
            }

            SSL_set_bio(m_ssl, sbio, sbio);
            sslConnectionStatus = SSL_connect(m_ssl);
            if (sslConnectionStatus <= 0) {
                // SSL Connect Error
                std::string msg;
                int errorCode = SSL_get_error(m_ssl, sslConnectionStatus);
                if (errorCode == SSL_ERROR_SSL) {
                    msg = std::string("SSL_connect failed: ");
                    msg.append(ERR_error_string(ERR_get_error(), NULL));
                } else {
                    msg = std::string("SSL_connect failed with code ");
                    std::ostringstream strbf;
                    strbf << errorCode;
                    msg.append(strbf.str());
                }

                //Shut down connection
                m_sslContext.flush();
                if (m_ssl != NULL) {
                    SSL_shutdown(m_ssl);
                    SSL_free(m_ssl);
                    m_ssl = NULL;
                    TCPSocket::close();
                    TCPSocket::connect(m_host, m_port);
                }

                // One retry is allowed because a certificate on the server or
                // client may have expired. The file may have been refreshed with
                // a new one, so it needs re-reading
                if (failedOnce) {
                    throw RGMATemporaryException(msg);
                }
                failedOnce = true;
            }

        }
    } while (sslConnectionStatus <= 0);
}

void SSLSocket::testRead(int returnCode) throw (RGMATemporaryException, RGMAPermanentException) {
    if (returnCode <= 0) {
        switch (SSL_get_error(m_ssl, returnCode)) {
        case SSL_ERROR_ZERO_RETURN:
            throw RGMATemporaryException("SSL_ERROR_ZERO_RETURN The TLS/SSL connection has been closed");
        case SSL_ERROR_WANT_READ:
            throw RGMATemporaryException("SSL_ERROR_WANT_READ The TLS/SSL The operation did not complete");
        case SSL_ERROR_WANT_WRITE:
            throw RGMATemporaryException("SSL_ERROR_WANT_WRITE The TLS/SSL The operation did not complete");
        case SSL_ERROR_WANT_CONNECT:
            throw RGMATemporaryException("SSL_ERROR_WANT_CONNECT The TLS/SSL The operation did not complete");
        case SSL_ERROR_WANT_ACCEPT:
            throw RGMATemporaryException("SSL_ERROR_WANT_ACCEPT The TLS/SSL The operation did not complete");
        case SSL_ERROR_WANT_X509_LOOKUP:
            throw RGMATemporaryException(
                    "SSL_ERROR_WANT_X509_LOOKUP The TLS/SSL The operation did not complete because an application callback set by SSL_CTX_set_client_cert_cb() has asked to be called again");
        case SSL_ERROR_SYSCALL:
            throw RGMATemporaryException("SSL_ERROR_SYSCALL");
        case SSL_ERROR_SSL:
            throw RGMATemporaryException(
                    "SSL_ERROR_SSL The TLS/SSL A failure in the SSL library occurred, usually a protocol error. The OpenSSL error queue contains more information on the error.");
        default:
            throw RGMAPermanentException("Unknown Errror code");
        }
    }
}

void SSLSocket::reMakeSSLConnection() throw (RGMATemporaryException, RGMAPermanentException) {
    if (m_ssl != NULL) {
        SSL_shutdown(m_ssl);
        SSL_free(m_ssl);
        m_ssl = NULL;
    }
    TCPSocket::close();
    TCPSocket::connect(m_host, m_port);
    establishSSL();
}

void SSLSocket::write(std::string request, std::string& response) throw (RGMATemporaryException,
        RGMAPermanentException) {
    const int BUFFER_SIZE = 16000;
    char * buffer = new char[BUFFER_SIZE];
    int pingStatus = sslPing(m_ssl, m_sock);
    if (pingStatus > 0) {
        if (pingStatus > 1) {
            SSL_shutdown(m_ssl);
        }
        if (m_ssl != NULL) {
            SSL_free(m_ssl);
            m_ssl = NULL;
        }
        TCPSocket::close();
        TCPSocket::connect(m_host, m_port);
        establishSSL();
    }

    // this is to try and counteract the close notify from tomcat
    if (connectionCounter > MAX_NO_OF_CONNECTIONS) {
        reMakeSSLConnection();
    }

    int bytesWritten = SSL_write(m_ssl, request.c_str(), request.length());
    connectionCounter++;

    if (bytesWritten <= 0) {
        int errorCode = SSL_get_error(m_ssl, bytesWritten);
        reMakeSSLConnection();
        throw RGMATemporaryException(strerror(errorCode));
    }

    signed int bytesRead = 0;
    bool chunked = false;
    bool close = false;
    std::string document;
    // do the first read
    bytesRead = SSL_read(m_ssl, buffer, BUFFER_SIZE);
    testRead(bytesRead);

    document += std::string(buffer, 0, bytesRead);

    // get the header and find out what type of transfer it is
    std::string header;
    if (document.size() < 3) {
        throw RGMAPermanentException("Internal error - found document with size < 3");
    }
    for (size_t index = 0; index < document.size() - 3; ++index) {
        if ((document[index] == 13) && (document[index + 1] == 10) && (document[index + 2] == 13) && (document[index
                + 3] == 10)) {
            header = document.substr(0, index + 4);
            document.erase(0, index + 4);
            break;
        }
    }
    response += header;
    if (header.find("TRANSFER-ENCODING: CHUNKED") != std::string::npos || header.find("Transfer-Encoding: chunked")
            != std::string::npos) {
        chunked = true;
        long lchunkSize = strtol(document.c_str(), NULL, 16);
        if (lchunkSize < 0) {
            throw RGMAPermanentException("Internal error - received document with negative chunk size");
        }
        unsigned long chunkSize = lchunkSize;
        while (true) {
            while (document.length() < chunkSize) {
                bytesRead = SSL_read(m_ssl, buffer, BUFFER_SIZE);
                testRead(bytesRead);
                document.append(std::string(buffer, 0, bytesRead));
            }
            if (chunkSize) {
                //find the length of the chunk descriptor
                int chunkDescriptorLength = document.find("\r\n", 0);
                /*add the chunk descriptor + the end of line characters*/
                response.append(document.substr(0, chunkDescriptorLength + 2));
                //remove the chunk descriptor and the end of line characters
                document.erase(0, chunkDescriptorLength + 2);
                //append the next chunk to the response
                response.append(document.substr(0, chunkSize + 2));
                //remove the chunk and the end of line characters
                document.erase(0, chunkSize + 2);
                if (document.length() == 0) {
                    bytesRead = SSL_read(m_ssl, buffer, BUFFER_SIZE);
                    testRead(bytesRead);
                    document.append(std::string(buffer, 0, bytesRead));
                }

                //get the next chunk size
                lchunkSize = strtol(document.c_str(), NULL, 16);
                if (lchunkSize < 0) {
                    throw RGMAPermanentException("Internal error - received document with negative chunk size");
                }
                chunkSize = lchunkSize;
            } else {
                // Zero chunk size marks end of input
                response.append(std::string(buffer, 0, chunkSize));

                break;
            }
        }
    } else if (header.find("Content-Length: ") != std::string::npos) {
        int length = atoi(header.substr(header.find("Content-Length: ") + 16, header.find("\n")).c_str());
        int bytesGot = document.size();
        while (bytesGot < length) {
            bytesRead = SSL_read(m_ssl, buffer, BUFFER_SIZE);
            testRead(bytesRead);

            bytesGot += bytesRead;
            document.append(std::string(buffer, 0, bytesRead));
        }
        response.append(document);
    } else {
        while ((bytesRead = SSL_read(m_ssl, buffer, BUFFER_SIZE)) > 0) {
            /* No point calling testRead here as we got data back */
            document.append(std::string(buffer, 0, bytesRead));
        }
        response.append(document);
    }

    if (bytesRead < 0) {
        // Try to shut the connection down with a close_notify
        if (m_ssl != NULL) {
            SSL_shutdown(m_ssl);
            SSL_free(m_ssl);
            TCPSocket::close();
            TCPSocket::connect(m_host, m_port);
            establishSSL();
        }
        testRead(bytesRead);
    }
    if (close) {
        SSL_shutdown(m_ssl);
        SSL_free(m_ssl);
        m_ssl = NULL;
        TCPSocket::close();
        TCPSocket::connect(m_host, m_port);
        establishSSL();
    }
    delete[] buffer;
    buffer = 0;
}

int SSLSocket::sslPing(SSL *ssl, int socket) {
    if (ssl == NULL) {
        return 1;
    }
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
        } else {
            return 2; /* other SSL notification or HTTP protocol error */
        }
    }
    return 0; /* Nothing to read, SSL connection is OK */
}

void SSLSocket::close() {
    if (m_ssl != NULL) {
        SSL_shutdown(m_ssl);
        SSL_free(m_ssl);
        m_ssl = NULL;
    }

    TCPSocket::close();
}
SSLContext SSLSocket::m_sslContext;
}
}
