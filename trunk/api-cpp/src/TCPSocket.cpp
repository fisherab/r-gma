#include "rgma/TCPSocket.h"

#include <netdb.h>
#include <errno.h>
#include <sstream>
#include <iostream>
#include <string.h>

namespace glite {
namespace rgma {

TCPSocket::TCPSocket() {
}

TCPSocket::~TCPSocket() {
    close();
}

void TCPSocket::connect(std::string host, int port) throw(RGMATemporaryException, RGMAPermanentException) {
    struct timeval tv;
    tv.tv_sec = 300;
    tv.tv_usec = 0;

    std::ostringstream portstring;
    portstring << port;

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM; //Reliable sequential stream
    hints.ai_flags = AI_ADDRCONFIG; // Only configure interfaces

    struct addrinfo *result;
    int retVal = getaddrinfo(host.c_str(), portstring.str().c_str(), &hints, &result);
    if (retVal != 0) {
        throw RGMAPermanentException("Error from getaddrinfo " + std::string(gai_strerror(retVal)) + " for host:port " + host
                + ":" + portstring.str());
    }
    for (struct addrinfo *ai = result; ai != NULL; ai = ai->ai_next) {
        m_sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (m_sock != -1) {
            if (setsockopt(m_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(timeval)) == 0
             && setsockopt(m_sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(timeval)) == 0
             && ::connect(m_sock, ai->ai_addr, ai->ai_addrlen) == 0) {
                break;
            } else {
                std::cerr << "Problem setting options or connecting: " << strerror(errno) << std::endl;
            }
            ::close(m_sock);
            m_sock = -1;
        }
    }
    freeaddrinfo(result);
    if (m_sock == -1) {
        throw RGMATemporaryException("Unable to make basic socket connection to host:port " + host + ":" + portstring.str());
    }
}

void TCPSocket::close() {
    if (::shutdown(m_sock, SHUT_RDWR) == 0) {
        ::close(m_sock);
    }

}

}
}
