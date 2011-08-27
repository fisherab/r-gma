/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API - SecondaryProducer.
 */

#include <stdio.h>   /* for NULL and FILE */
#include <string.h>  /* for strcmp */
#include <stdlib.h>  /* for malloc, free */

#include "rgma.h"
#include "rgma_command.h"
#include "rgma_lib.h"
#include "rgma_private.h"

PRIVATE void freeResource(RGMASecondaryProducer *);
PRIVATE void doDeclareTable(RGMASecondaryProducer *, const char *, const char *, int, RGMAException **);
PRIVATE void exterminate(RGMASecondaryProducer *, char *, RGMAException **);
PRIVATE void restore(RGMASecondaryProducer *, RGMAException **);

/** Frees an RGMASecondaryProducer */
PRIVATE void freeResource(RGMASecondaryProducer *r) {

    SPTable *table, *next;

    if (r) {
        lib_free(r->url);
        if (r->bsock != NULL) {
            tcp_close(r->bsock);
        }
        lib_free(r->logicalName);

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
PRIVATE void exterminate(RGMASecondaryProducer *r, char *cmd, RGMAException **exceptionPP) {
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

PRIVATE void restore(RGMASecondaryProducer *r, RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    const char *parameters[20];
    int n;
    SPTable * table;

    *exceptionPP = NULL;

    n = 0;
    parameters[n++] = "type";
    if (r->storage == RGMAStorageType_MEMORY)
        parameters[n++] = "memory";
    else
        parameters[n++] = "database"; /* RGMAStorageType_DATABASE */
    if (r->logicalName != NULL) {
        parameters[n++] = "logicalName";
        parameters[n++] = r->logicalName;
    }

    parameters[n++] = "isHistory";
    parameters[n++] = (r->supportedQueries == RGMASupportedQueries_CH || r->supportedQueries
            == RGMASupportedQueries_CHL) ? "true" : "false";

    parameters[n++] = "isLatest";
    parameters[n++] = (r->supportedQueries == RGMASupportedQueries_CL || r->supportedQueries
            == RGMASupportedQueries_CHL) ? "true" : "false";

    rs = sendCommand(&(r->bsock), r->url, "/createSecondaryProducer", n / 2, parameters, exceptionPP);
    if (*exceptionPP) {
        return;
    }

    r->connectionId = lib_TupleSetToInt(rs, exceptionPP);

    table = r->tables;
    while (table) {
        doDeclareTable(r, table->name, table->predicate, table->hrp, exceptionPP);
        if (*exceptionPP) {
            return;
        }
        table = table->next;
    }
}

PUBLIC
RGMASecondaryProducer *RGMASecondaryProducer_create(RGMAStorageType storageType, char *logicalName,
        RGMASupportedQueries supportedQueries, RGMAException **exceptionPP) {
    RGMASecondaryProducer *r;

    *exceptionPP = NULL;

    /* Use calloc to clear memory so that can free it easily if things go wrong */
    r = (RGMASecondaryProducer *) calloc(1, sizeof(RGMASecondaryProducer));
    if (r == NULL) {
        return NULL; /* Nothing else we can do. */
    }

    r->storage = storageType;
    if (logicalName) {
        r->logicalName = (char *) malloc(strlen(logicalName) + 1);
        strcpy(r->logicalName, logicalName);
    }
    r->supportedQueries = supportedQueries;

    if (storageType != RGMAStorageType_MEMORY && storageType != RGMAStorageType_DATABASE) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid storage type", 0);
        freeResource(r);
        return NULL;
    }

    r->url = lib_getServiceURL("SecondaryProducerServlet", exceptionPP);
    if (*exceptionPP) {
        freeResource(r);
        return NULL;
    }

    if (supportedQueries != RGMASupportedQueries_C && supportedQueries != RGMASupportedQueries_CH && supportedQueries
            != RGMASupportedQueries_CL && supportedQueries != RGMASupportedQueries_CHL) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid value for supportedQueries", 0);
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
void RGMASecondaryProducer_close(RGMASecondaryProducer *r, RGMAException **exceptionPP) {
    exterminate(r, "/close", exceptionPP);
}

PUBLIC
void RGMASecondaryProducer_destroy(RGMASecondaryProducer *r, RGMAException **exceptionPP) {
    exterminate(r, "/destroy", exceptionPP);
}

PUBLIC RGMAResourceEndpoint * RGMASecondaryProducer_getResourceEndpoint(RGMASecondaryProducer *r,
        RGMAException **exceptionPP) {

    RGMAResourceEndpoint * ep;

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMASecondaryProducer pointer is NULL", 0);
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

PUBLIC
int RGMASecondaryProducer_getResourceId(RGMASecondaryProducer *r, RGMAException **exceptionPP) {

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMASecondaryProducer pointer is NULL", 0);
        return 0;
    }

    *exceptionPP = NULL;

    return r->connectionId;

}

PRIVATE void doDeclareTable(RGMASecondaryProducer *r, const char *name, const char *predicate, int hrpSec,
        RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    STRINGINT connectionId_s, hrpSec_s;
    const char *parameters[20];
    int n;

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, r->connectionId);
    parameters[n++] = "tableName";
    parameters[n++] = name;
    parameters[n++] = "predicate";
    parameters[n++] = predicate;
    parameters[n++] = "hrpSec";
    parameters[n++] = ITOA(hrpSec_s, hrpSec);

    rs = sendCommand(&(r->bsock), r->url, "/declareTable", n / 2, parameters, exceptionPP);

    if (*exceptionPP) {
        return;
    }
    lib_freeTupleSet(rs);
}

PUBLIC
void RGMASecondaryProducer_declareTable(RGMASecondaryProducer *r, const char *name, const char *predicate, int hrpSec,
        RGMAException **exceptionPP) {

    SPTable * table;

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMASecondaryProducer pointer is NULL", 0);
        return;
    }

    *exceptionPP = NULL;

    doDeclareTable(r, name, predicate, hrpSec, exceptionPP);
    if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
        restore(r, exceptionPP);
        if (*exceptionPP) {
            if ((*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
                (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
            }
            return;
        }
        doDeclareTable(r, name, predicate, hrpSec, exceptionPP);
        if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
        }

    }
    if (*exceptionPP) {
        return;
    }

    table = (SPTable *) malloc(sizeof(SPTable));
    if (!table) {
        lib_setOutOfMemoryException(exceptionPP);
        return;
    }
    table->name = lib_dupString(name, exceptionPP);
    table->predicate = lib_dupString(predicate, exceptionPP);
    table->hrp = hrpSec;
    table->next = r->tables;
    if (*exceptionPP) {
        lib_free(table->name);
        lib_free(table->predicate);
        lib_free(table);
    } else {
        r->tables = table;
    }
}

PRIVATE void doShowSignOfLife(RGMASecondaryProducer *r, RGMAException ** exceptionPP) {

    RGMATupleSet *rs;
    STRINGINT connectionId_s;
    const char *parameters[20];
    int n;

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, r->connectionId);

    rs = sendCommand(&(r->bsock), r->url, "/showSignOfLife", n / 2, parameters, exceptionPP);

    if (*exceptionPP) {
        return;
    }
    lib_freeTupleSet(rs);
}

PUBLIC void RGMASecondaryProducer_showSignOfLife(RGMASecondaryProducer *r, RGMAException ** exceptionPP) {

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMASecondaryProducer pointer is NULL", 0);
        return;
    }

    *exceptionPP = NULL;

    doShowSignOfLife(r, exceptionPP);
    if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
        restore(r, exceptionPP);
        /* No need to send a showSignOfLife - just restore */
        if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
        }
    }
}

PUBLIC int RGMASecondaryProducer_staticShowSignOfLife(int resourceId, RGMAException ** exceptionPP) {
    RGMATupleSet *rs;
    STRINGINT connectionId_s;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SecondaryProducerServlet", exceptionPP);
    if (*exceptionPP) {
        return 0;
    }

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, resourceId);

    rs = sendCommand(NULL, url, "/showSignOfLife", n / 2, parameters, exceptionPP);

    if (*exceptionPP) {
        if ((*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            lib_freeException(*exceptionPP);
            *exceptionPP = NULL;
        }
        return 0;
    }

    lib_freeTupleSet(rs);
    return 1;
}
