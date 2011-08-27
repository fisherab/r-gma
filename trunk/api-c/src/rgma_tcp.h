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

#ifndef RGMA_TCP_H
#define RGMA_TCP_H

/* Header for rgma_tcp.c. */

#define TCP_NETBUFSIZ 2048

typedef struct BUFFERED_SOCKET_S BUFFERED_SOCKET;

extern int tcp_connect(const char *url, int port, BUFFERED_SOCKET **bsock, char** erm);
extern void tcp_close(BUFFERED_SOCKET *bsock);
extern int tcp_readLine(BUFFERED_SOCKET *bsock, char *, int, int);
extern int tcp_readBytes(BUFFERED_SOCKET *bsock, char *, int);
extern int tcp_writeBytes(BUFFERED_SOCKET *bsock, char *, int);

#endif /* RGMA_TCP_H */
