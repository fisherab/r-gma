/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API - OnDemandProducer.
 */

#include <stdio.h>   /* for NULL and FILE */
#include <string.h>  /* for strcmp */
#include <stdlib.h>  /* for malloc, free */

#include "rgma.h"
#include "rgma_command.h"
#include "rgma_lib.h"
#include "rgma_private.h"

PRIVATE void freeResource(RGMAOnDemandProducer *);
PRIVATE void doDeclareTable(RGMAOnDemandProducer *, const char *, const char *, RGMAException **);
PRIVATE void exterminate(RGMAOnDemandProducer *, char *, RGMAException **);
PRIVATE void restore(RGMAOnDemandProducer *, RGMAException **);

/** Frees an RGMAOnDemandProducer */
PRIVATE void freeResource(RGMAOnDemandProducer *r) {

    ODPTable *table, *next;

    if (r) {
        lib_free(r->url);
        if (r->bsock != NULL) {
            tcp_close(r->bsock);
        }
        lib_free(r->hostName);

        table = r->tables;
        while (table) {
            lib_free(table->name);
            lib_free(table->predicate);
            next = table->next;
            lib_free(table);
            table = next;
        }
        lib_free(r); /* Must be last */
    }
}

/** Called by close and destroy */
PRIVATE void exterminate(RGMAOnDemandProducer *r, char *cmd, RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    const char *parameters[20];
    int n;
    STRINGINT connectionId_s;

    *exceptionPP = NULL;

    if (r) {
        n = 0;
        parameters[n++] = "connectionId";
        parameters[n++] = ITOA(connectionId_s, r->connectionId);

        rs = sendCommand(&(r->bsock), r->url, cmd, n / 2, parameters, exceptionPP);
        if (rs) {
            lib_freeTupleSet(rs);
            freeResource(r);
        }
    }
}

PRIVATE void restore(RGMAOnDemandProducer *r, RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    const char *parameters[20];
    int n;
    ODPTable * table;
    STRINGINT port_s;

    *exceptionPP = NULL;

    n = 0;
    parameters[n++] = "hostName";
    parameters[n++] = r->hostName;
    parameters[n++] = "port";
    parameters[n++] = ITOA(port_s, r->port);

    rs = sendCommand(&(r->bsock), r->url, "/createOnDemandProducer", n / 2, parameters, exceptionPP);
    if (*exceptionPP) {
        return;
    }

    r->connectionId = lib_TupleSetToInt(rs, exceptionPP);

    table = r->tables;
    while (table) {
        doDeclareTable(r, table->name, table->predicate, exceptionPP);
        if (*exceptionPP) {
            return;
        }
        table = table->next;
    }
}

PUBLIC
RGMAOnDemandProducer *RGMAOnDemandProducer_create(const char *hostName, int port, RGMAException **exceptionPP) {

    RGMAOnDemandProducer *r;

    *exceptionPP = NULL;

    /* Use calloc to clear memory so that can free it easily if things go wrong */
    r = (RGMAOnDemandProducer *) calloc(1, sizeof(RGMAOnDemandProducer));
    if (r == NULL) {
        return NULL; /* Nothing else we can do. */
    }

    r->hostName = lib_dupString(hostName, exceptionPP);
    r->port = port;

    r->url = lib_getServiceURL("OnDemandProducerServlet", exceptionPP);
    if (*exceptionPP) {
        freeResource(r);
        return NULL;
    }

    restore(r, exceptionPP);

    if (*exceptionPP) {
        freeResource(r);
        return NULL;
    }

    return r;
}

PUBLIC
void RGMAOnDemandProducer_close(RGMAOnDemandProducer *r, RGMAException **exceptionPP) {
    exterminate(r, "/close", exceptionPP);
}

PUBLIC
void RGMAOnDemandProducer_destroy(RGMAOnDemandProducer *r, RGMAException **exceptionPP) {
    exterminate(r, "/destroy", exceptionPP);
}

PRIVATE void doDeclareTable(RGMAOnDemandProducer *r, const char *name, const char *predicate,
        RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    STRINGINT connectionId_s;
    const char *parameters[20];
    int n;

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, r->connectionId);
    parameters[n++] = "tableName";
    parameters[n++] = name;
    parameters[n++] = "predicate";
    parameters[n++] = predicate;

    rs = sendCommand(&(r->bsock), r->url, "/declareTable", n / 2, parameters, exceptionPP);

    if (*exceptionPP) {
        return;
    }
    lib_freeTupleSet(rs);
}

PUBLIC
void RGMAOnDemandProducer_declareTable(RGMAOnDemandProducer *r, const char *name, const char *predicate,
        RGMAException **exceptionPP) {

    ODPTable * table;

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAOnDemandProducer pointer is NULL", 0);
        return;
    }

    *exceptionPP = NULL;

    doDeclareTable(r, name, predicate, exceptionPP);
    if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
        restore(r, exceptionPP);
        if (*exceptionPP) {
            if ((*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
                (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
            }
            return;
        }
        doDeclareTable(r, name, predicate, exceptionPP);
        if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
        }
    }
    if (*exceptionPP) {
        return;
    }

    table = (ODPTable *) malloc(sizeof(ODPTable));
    if (!table) {
        lib_setOutOfMemoryException(exceptionPP);
        return;
    }
    table->name = lib_dupString(name, exceptionPP);
    table->predicate = lib_dupString(predicate, exceptionPP);
    table->next = r->tables;
    if (*exceptionPP) {
        lib_free(table->name);
        lib_free(table->predicate);
        lib_free(table);
    } else {
        r->tables = table;
    }
}

PUBLIC RGMAResourceEndpoint * RGMAOnDemandProducer_getResourceEndpoint(RGMAOnDemandProducer *r,
        RGMAException **exceptionPP) {

    RGMAResourceEndpoint * ep;

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAOnDemandProducer pointer is NULL", 0);
        return 0;
    }

    *exceptionPP = NULL;

    ep = (RGMAResourceEndpoint *) malloc(sizeof(RGMAResourceEndpoint));
    if (!ep) {
        lib_setOutOfMemoryException(exceptionPP);
        return NULL;
    }
    ep->resourceId = r->connectionId;
    ep->url = lib_dupString(r->url, exceptionPP);
    if (*exceptionPP) {
        return NULL;
    }

    return ep;
}

