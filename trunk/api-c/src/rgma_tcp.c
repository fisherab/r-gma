/*
 *  Copyright (c) 2004 on behalf of the EU EGEE Project:
 *  The European Organization for Nuclear Research (CERN),
 *  Istituto Nazionale di Fisica Nucleare (INFN), Italy
 *  Datamat Spa, Italy
 *  Centre National de la Recherche Scientifique (CNRS), France
 *  CS Systeme d'Information (CSSI), France
 *  Royal Institute of Technology, Center for Parallel Computers (KTH-PDC), Sweden
 *  Universiteit van Amsterdam (UvA), Netherlands
 *  University of Helsinki (UH.HIP), Finland
 *  University of Bergen (UiB), Norway
 *  Council for the Central Laboratory of the Research Councils (CCLRC), United Kingdom
 */

/* This small module handles the TCP connection */

#include <stdio.h>  /* for NULL */
#include <string.h> /* for memcpy, memset */
#include <stdlib.h> /* for free */

#define PRIVATE static
#define PUBLIC /* empty */

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#include "rgma_ssl.h" /* for rgmassl_ functions (for secure HTTP) */
#include "rgma_tcp.h"

struct BUFFERED_SOCKET_S {
    int socket;
    RGMA_SSL ssl;
    char *buf;
    int num_left;
    int next;
};

PRIVATE int read_byte(BUFFERED_SOCKET *, int *);
PRIVATE int connectWithTimeout(int, const struct sockaddr *, socklen_t, int);

/* PUBLIC FUNCTIONS ***********************************************************/

/**
 * Connects to specified host/port and initiates SSL connection if requested.
 *
 * If *bsockP is not NULL, only creates a new connection if rgmassl_ping()
 * fails (so socket can be reused).
 *
 * @param  host       Hostname to which to make connection.
 * @param  port       Port number on which TCP service is listening.
 * @param  bsock      Address of buffered socket to be filled in.
 *
 * @return  0 on success,
 *         -3 for authentication failure,
 *         -2 for socket (connect/read/write) timeout,
 *         -1 for all other errors.
 */

PUBLIC int tcp_connect(const char *host, int port, BUFFERED_SOCKET **bsockP, char** erm) {
    BUFFERED_SOCKET *bsock;
    int sock = -1;
    RGMA_SSL ssl;
    struct addrinfo hints;
    struct addrinfo *ai;

    /* If attempting to reuse an existing connection,
     check that it hasn't timed out before reusing it. */

    if (*bsockP) {/* connection already exists */
        bsock = *bsockP;
        if (rgmassl_ping(bsock->ssl, bsock->socket) == 0) {
            return 0; /* connection is OK */
        }

        tcp_close(bsock); /* abandon this connection */
        *bsockP = NULL;
    }

    /* Initialise new buffered socket structure. */

    bsock = (BUFFERED_SOCKET *) malloc(sizeof(BUFFERED_SOCKET));
    if (bsock == NULL)
        return -1;
    bsock->socket = -1;
    bsock->ssl = NULL;
    bsock->buf = (char *) malloc(TCP_NETBUFSIZ);
    if (bsock->buf == NULL) {
        free(bsock);
        return -1; /* connect failed */
    }
    bsock->num_left = 0;
    bsock->next = 0;

    /* Set up the address structures. */

    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM; //Reliable sequential stream
    hints.ai_flags = AI_ADDRCONFIG; // Only configure interfaces

    struct addrinfo *result;
    int retVal = getaddrinfo(host, "8443", &hints, &result);
    if (retVal != 0) {
        return -1;
    }
    for (ai = result; ai != NULL; ai = ai->ai_next) {
        sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock != -1) {
            if (connectWithTimeout(sock, ai->ai_addr, ai->ai_addrlen, 300) == 0) {
                break;
            }
            close(sock);
            sock = -1;
        }
    }
    freeaddrinfo(result);
    if (sock == -1) {
        return -1;
    }

    /* Establish the SSL connection. */

    ssl = NULL;
    if (!(ssl = rgmassl_connect(sock, erm))) {
        close(sock);
        return -3; /* authentication failed */
    }

    /* Save connection details in the buffered socket. */

    bsock->socket = sock;
    bsock->ssl = ssl;
    *bsockP = bsock;

    return 0;
}

/**
 * Shuts down SSL connection, closes socket and frees up memory.
 *
 * @param  bsock      Buffered socket to be closed.
 *
 * @return Nothing.
 */

PUBLIC void tcp_close(BUFFERED_SOCKET *bsock) {
    if (bsock == NULL)
        return; /* nothing to do */
    if (bsock->ssl) {
        rgmassl_shutdown(bsock->ssl);
        rgmassl_free(bsock->ssl);
    }
    if (bsock->socket != -1) {
        close(bsock->socket);
    }
    if (bsock->buf) {
        free(bsock->buf);
    }
    free(bsock);
}

/**
 * Reads the next line from the socket. Always reads a complete line, but
 * only stores at most "size" bytes in "buf", including a terminating "\0".
 * If the line is short enough to fit into "buf", the terminating "\r\n" is
 * is converted into a single "\n" and appended to the string. If line is
 * longer than "max", give up (to avoid infinite loops).
 *
 * @param  bs         Buffered socket from which to read.
 * @param  buf        Buffer into store the line.
 * @param  size       Size of the buffer (bytes).
 * @param  max        Maximum number of bytes to read (failsafe, e.g. 1000000)
 *
 * @return Number of bytes read or
 *          0 (server shut connection) or
 *         -2 (read timed out) or
 *         -1 (other read error).
 */

PUBLIC int tcp_readLine(BUFFERED_SOCKET *bs, char *buf, int size, int max) {
    int i, last_byte, this_byte, n;

    i = 0;
    last_byte = 0;
    while (max--) {
        n = read_byte(bs, &this_byte);
        if (n != 1)
            return n; /* read error of some sort */

        if (last_byte == '\r' && this_byte == '\n') {
            if (i > 0 && i < size)
                buf[i - 1] = '\n';
            break;
        } else if (i < (size - 1))
            buf[i++] = this_byte;

        last_byte = this_byte;
    }
    if (max == 0)
        return -1;

    buf[i] = '\0';
    return i;
}

/**
 * Reads at most "size" bytes from socket into "buf".
 *
 * @param  bs         Buffered socket from which to read.
 * @param  buf        Buffer into which to read.
 * @param  size       Size of the buffer (bytes).
 *
 * @return Number of bytes read (which may be less than requested if the
 *         buffer's empty and the server's shut the connection) or -2 (socket
 *         read timed out) or -1 (some other socket read error occurred).
 */

PUBLIC int tcp_readBytes(BUFFERED_SOCKET *bs, char *buf, int size) {
    int i, this_byte, n;

    for (i = 0; i < size; ++i) {
        n = read_byte(bs, &this_byte);
        if (n < 0)
            return n;
        else if (n == 0)
            return i; /* number read so far */
        else
            buf[i] = this_byte;
    }
    return i;
}

/**
 * Writes "size" bytes from "buf" into socket.
 *
 * @param  bs         Buffered socket to which to write.
 * @param  buf        Buffer to write.
 * @param  size       Size of the buffer (bytes).
 *
 * @return 0 on success, -2 if write times out, -1 on any other error.
 */

PUBLIC int tcp_writeBytes(BUFFERED_SOCKET *bs, char *buf, int size) {
    int n = rgmassl_send(bs->ssl, buf, size);
    if (n == -1 && errno == EAGAIN) {
        return -2; /* write timed out */
    }
    if (n <= 0) {
        return -1;
    }
    return 0;
}

/* PRIVATE FUNCTIONS **********************************************************/

/**
 * Reads one byte from buffer.
 *
 * @param bs Buffered socket from which to read.
 *        b  Byte into which to write
 *
 * @return  1 on success (one byte read)
 *          0 if server shut connection
 *         -2 if socket read timed out
 *         -1 if other socket read error occurred
 */

PRIVATE int read_byte(BUFFERED_SOCKET *bs, int *b) {
    int n;

    if (bs->num_left == 0) {
        n = rgmassl_recv(bs->ssl, bs->buf, TCP_NETBUFSIZ);
        if (n == -1 && errno == EAGAIN) {
            return -2; /* read timed out */
        }
        if (n < 0) {
            return -1; /* other read error */
        }
        if (n == 0) {
            return 0; /* server shut connection */
        }
        bs->num_left = n;
        bs->next = 0;
    }

    *b = bs->buf[bs->next];
    --bs->num_left;
    ++bs->next;

    return 1; /* successfully read byte from buffer */
}

/**
 * Connects socket to remote host, but gives up if timeout expires. See the
 * connect(2) man page for an explanation of the use of select() for this.
 *
 * For convenience, we also add the read/write timeouts to the socket here.
 *
 * @param sock    Socket to connect.
 *        addr    Pointer to address structure representing remote host.
 *        addrlen Size of address structure.
 *        timeout Timeout in seconds.
 *
 * @return 0 on success, -2 on timeout, -1 on any other error
 */

PRIVATE int connectWithTimeout(int sock, const struct sockaddr *addr, socklen_t addrlen, int timeout) {
    struct timeval tv;
    fd_set wfds;
    int retval, optval;
    socklen_t optlen;
    long flags;

    /* Make existing socket temporarily non-blocking. */

    flags = fcntl(sock, F_GETFL);
    if (flags == -1)
        return -1;
    if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1)
        return -1;

    /* Do non-blocking connect to remote host - this will return immediately
     with a -1 return value, but if errno is set to EINPROGRESS, all is
     well. */

    if (connect(sock, addr, addrlen) == -1) {
        if (errno != EINPROGRESS)
            return -1;
    }

    /* Set up select() to watch for writability of the socket: select will
     block with the specified timeout. Select will return 1 when the
     connect is complete or fails - the value of SO_ERROR will tell us
     which has occurred. */

    FD_ZERO(&wfds);
    FD_SET(sock, &wfds);
    tv.tv_sec = timeout;
    tv.tv_usec = 0;
    retval = select(sock + 1, NULL, &wfds, NULL, &tv);
    if (retval == 0)
        return -2; /* timed out before connect completed */

    optlen = sizeof(optval);
    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &optval, &optlen) == -1) {
        return -1;
    }
    if (optval != 0)
        return -1; /* connect failed */

    /* Restore original blockingness. */

    if (fcntl(sock, F_SETFL, flags) == -1)
        return -1;

    /* Establish read and write timeouts on the socket. Note that these
     affect OpenSSL's reads and writes as well as ours, so there's a
     small chance that they will fire during the SSL connect, causing us
     to report an authentication failure instead of a simple timeout. Ho
     hum. Ignore any errors: if it doesn't work, it doesn't work. */

    tv.tv_sec = timeout;
    tv.tv_usec = 0;
    optlen = sizeof(tv);
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, optlen);
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, optlen);

    return 0;
}
