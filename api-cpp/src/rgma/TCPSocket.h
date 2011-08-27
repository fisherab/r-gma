/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMA_TCPSOCKET_H
#define RGMA_TCPSOCKET_H

#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"

#include <string>

namespace glite {
namespace rgma {
/**
 * TCP socket wrapper
 */
class TCPSocket {
    protected:
        int m_sock;
    public:
        virtual ~TCPSocket();
        TCPSocket();
        virtual void connect(std::string host, int port) throw(RGMATemporaryException, RGMAPermanentException);
        virtual void close();
};
}
}
#endif
