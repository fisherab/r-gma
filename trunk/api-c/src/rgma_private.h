/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API - Consumer.
 */

#ifndef RGMA_PRIVATE_H
#define RGMA_PRIVATE_H

#define PRIVATE static
#define PUBLIC /* empty */

struct PPTable_S {
    char * name;
    char * predicate;
    int hrp;
    int lrp;
    struct PPTable_S * next; /* Link to next table or NULL for the end */
};

typedef struct PPTable_S PPTable;

struct RGMAPrimaryProducer_S {
    char *url;
    int connectionId;
    BUFFERED_SOCKET *bsock;
    int supportedQueries;
    int storage;
    char * logicalName;
    PPTable * tables; /* Linked list of tables */
};

struct SPTable_S {
    char * name;
    char * predicate;
    int hrp;
    struct SPTable_S * next; /* Link to next table or NULL for the end */
};

typedef struct SPTable_S SPTable;

struct RGMASecondaryProducer_S {
    char *url;
    int connectionId;
    BUFFERED_SOCKET *bsock;
    int supportedQueries;
    int storage;
    char * logicalName;
    SPTable * tables; /* Linked list of tables */
};

struct ODPTable_S {
    char * name;
    char * predicate;
    struct ODPTable_S * next; /* Link to next table or NULL for the end */
};

typedef struct ODPTable_S ODPTable;

struct RGMAOnDemandProducer_S {
    char *url;
    int connectionId;
    BUFFERED_SOCKET *bsock;
    char * hostName;
    int port;
    ODPTable * tables; /* Linked list of tables */
};

struct RGMAConsumer_S {
    char *url;
    int connectionId;
    BUFFERED_SOCKET *bsock;
    char* query;
    char* queryType;
    int queryInterval;
    int timeout;
    int numProducers;
    char ** producers;
    int eof;
};

/* Convenience type/macro to convert integers to strings. */
typedef char STRINGINT[11];
#define ITOA(a, i) (sprintf((a), "%d", (i)), (a))

#endif /* RGMA_PRIVATE_H */
