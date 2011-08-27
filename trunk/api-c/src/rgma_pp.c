/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API - PrimaryProducer.
 */

#include <stdio.h>   /* for NULL and FILE */
#include <string.h>  /* for strcmp */
#include <stdlib.h>  /* for malloc, free */

#include "rgma.h"
#include "rgma_command.h"
#include "rgma_lib.h"
#include "rgma_private.h"

PRIVATE void freeResource(RGMAPrimaryProducer *);
PRIVATE void doDeclareTable(RGMAPrimaryProducer *, const char *, const char *, int, int, RGMAException **);
PRIVATE void exterminate(RGMAPrimaryProducer *, char *, RGMAException **);
PRIVATE void restore(RGMAPrimaryProducer *, RGMAException **);
PRIVATE void doInsert(RGMAPrimaryProducer *, const char *, int, RGMAException **);
PRIVATE void doInsertList(RGMAPrimaryProducer *, int, char **, int, RGMAException **);

/** Frees an RGMAPrimaryProducer */
PRIVATE void freeResource(RGMAPrimaryProducer *r) {

    PPTable *table, *next;

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
PRIVATE void exterminate(RGMAPrimaryProducer *r, char *cmd, RGMAException **exceptionPP) {
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

PRIVATE void restore(RGMAPrimaryProducer *r, RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    const char *parameters[20];
    int n;
    PPTable * table;

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

    rs = sendCommand(&(r->bsock), r->url, "/createPrimaryProducer", n / 2, parameters, exceptionPP);
    if (*exceptionPP) {
        return;
    }

    r->connectionId = lib_TupleSetToInt(rs, exceptionPP);

    table = r->tables;
    while (table) {
        doDeclareTable(r, table->name, table->predicate, table->hrp, table->lrp, exceptionPP);
        if (*exceptionPP) {
            return;
        }
        table = table->next;
    }
}

PUBLIC
RGMAPrimaryProducer *RGMAPrimaryProducer_create(RGMAStorageType storageType, char *logicalName,
        RGMASupportedQueries supportedQueries, RGMAException **exceptionPP) {
    RGMAPrimaryProducer *r;

    *exceptionPP = NULL;

    /* Use calloc to clear memory so that can free it easily if things go wrong */
    r = (RGMAPrimaryProducer *) calloc(1, sizeof(RGMAPrimaryProducer));
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

    r->url = lib_getServiceURL("PrimaryProducerServlet", exceptionPP);
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
void RGMAPrimaryProducer_close(RGMAPrimaryProducer *r, RGMAException **exceptionPP) {
    exterminate(r, "/close", exceptionPP);
}

PUBLIC
void RGMAPrimaryProducer_destroy(RGMAPrimaryProducer *r, RGMAException **exceptionPP) {
    exterminate(r, "/destroy", exceptionPP);
}

PRIVATE void doDeclareTable(RGMAPrimaryProducer *r, const char *name, const char *predicate, int hrpSec, int lrpSec,
        RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    STRINGINT connectionId_s, hrpSec_s, lrpSec_s;
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
    parameters[n++] = "lrpSec";
    parameters[n++] = ITOA(lrpSec_s, lrpSec);

    rs = sendCommand(&(r->bsock), r->url, "/declareTable", n / 2, parameters, exceptionPP);

    if (*exceptionPP) {
        return;
    }
    lib_freeTupleSet(rs);
}

PUBLIC
void RGMAPrimaryProducer_declareTable(RGMAPrimaryProducer *r, const char *name, const char *predicate, int hrpSec,
        int lrpSec, RGMAException **exceptionPP) {

    PPTable * table;

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAPrimaryProducer pointer is NULL", 0);
        return;
    }

    *exceptionPP = NULL;

    doDeclareTable(r, name, predicate, hrpSec, lrpSec, exceptionPP);
    if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
        restore(r, exceptionPP);
        if (*exceptionPP) {
            if ((*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
                (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
            }
            return;
        }
        doDeclareTable(r, name, predicate, hrpSec, lrpSec, exceptionPP);
        if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
        }
    }

    if (*exceptionPP) {
        return;
    }

    table = (PPTable *) malloc(sizeof(PPTable));
    if (!table) {
        lib_setOutOfMemoryException(exceptionPP);
        return;
    }
    table->name = lib_dupString(name, exceptionPP);
    table->predicate = lib_dupString(predicate, exceptionPP);
    table->hrp = hrpSec;
    table->lrp = lrpSec;
    table->next = r->tables;
    if (*exceptionPP) {
        lib_free(table->name);
        lib_free(table->predicate);
        lib_free(table);
    } else {
        r->tables = table;
    }
}

PRIVATE void doInsert(RGMAPrimaryProducer *r, const char *insertStatement, int lrpSec, RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    STRINGINT connectionId_s, lrpSec_s;
    const char *parameters[20];
    int n;

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, r->connectionId);
    parameters[n++] = "insert";
    parameters[n++] = insertStatement;
    if (lrpSec != 0) {
        parameters[n++] = "lrpSec";
        parameters[n++] = ITOA(lrpSec_s, lrpSec);
    }

    rs = sendCommand(&(r->bsock), r->url, "/insert", n / 2, parameters, exceptionPP);
    if (*exceptionPP) {
        return;
    }
    lib_freeTupleSet(rs);
}

PUBLIC
void RGMAPrimaryProducer_insert(RGMAPrimaryProducer *r, const char *insertStatement, int lrpSec,
        RGMAException **exceptionPP) {

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAPrimaryProducer pointer is NULL", 0);
        return;
    }

    *exceptionPP = NULL;

    doInsert(r, insertStatement, lrpSec, exceptionPP);
    if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
        restore(r, exceptionPP);
        if (*exceptionPP) {
            if ((*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
                (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
            }
            return;
        }
        doInsert(r, insertStatement, lrpSec, exceptionPP);
        if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
        }
    }
}

PRIVATE void doInsertList(RGMAPrimaryProducer *r, int numInserts, char **insertStatements, int lrpSec,
        RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    STRINGINT connectionId_s, lrpSec_s;
    int n, i;
    const char **parameters;

    parameters = (const char **) malloc((4 + numInserts * 2) * sizeof(char *));
    if (parameters == NULL) {
        lib_setOutOfMemoryException(exceptionPP);
        return;
    }

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, r->connectionId);
    for (i = 0; i < numInserts; ++i) {
        parameters[n++] = "insert";
        parameters[n++] = insertStatements[i];
    }
    if (lrpSec != 0) {
        parameters[n++] = "lrpSec";
        parameters[n++] = ITOA(lrpSec_s, lrpSec);
    }

    rs = sendCommand(&(r->bsock), r->url, "/insert", n / 2, parameters, exceptionPP);
    lib_free(parameters);
    if (*exceptionPP) {
        return;
    }
    lib_freeTupleSet(rs);
}

PUBLIC void RGMAPrimaryProducer_insertList(RGMAPrimaryProducer *r, int numInserts, char **insertStatements, int lrpSec,
        RGMAException **exceptionPP) {

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAPrimaryProducer pointer is NULL", 0);
        return;
    }

    *exceptionPP = NULL;

    doInsertList(r, numInserts, insertStatements, lrpSec, exceptionPP);
    if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
        restore(r, exceptionPP);
        if (*exceptionPP) {
            if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
                (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
            }
            return;
        }
        doInsertList(r, numInserts, insertStatements, lrpSec, exceptionPP);
        if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
        }
    }
}

PUBLIC RGMAResourceEndpoint * RGMAPrimaryProducer_getResourceEndpoint(RGMAPrimaryProducer *r,
        RGMAException **exceptionPP) {

    RGMAResourceEndpoint * ep;

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAPrimaryProducer pointer is NULL", 0);
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

