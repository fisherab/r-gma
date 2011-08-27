/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

#ifndef RGMA_SSL_H
#define RGMA_SSL_H

/* Header for rgma_ssl.c */

typedef void *RGMA_SSL;

extern void rgmassl_cleanup(void);
extern RGMA_SSL rgmassl_connect(int socket, char** erm);
extern int rgmassl_recv(RGMA_SSL ssl, void *buf, int num);
extern int rgmassl_send(RGMA_SSL ssl, const void *buf, int num);
extern int rgmassl_shutdown(RGMA_SSL ssl);
extern void rgmassl_free(RGMA_SSL ssl);
extern int rgmassl_ping(RGMA_SSL ssl, int socket);

#endif /* RGMA_SSL_H */
