/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API - Consumer.
 */

#include <stdio.h>   /* for NULL and FILE */
#include <string.h>  /* for strcmp */
#include <stdlib.h>  /* for malloc, free */

#include "rgma.h"
#include "rgma_command.h"
#include "rgma_lib.h"
#include "rgma_private.h"

PRIVATE void freeResource(RGMAConsumer *);
PRIVATE void exterminate(RGMAConsumer *, char *, RGMAException **);
PRIVATE void restore(RGMAConsumer *, RGMAException **);
PRIVATE void doAbort(RGMAConsumer *, RGMAException **);
PRIVATE int doHasAborted(RGMAConsumer *, RGMAException **);
PRIVATE RGMATupleSet * doPop(RGMAConsumer *, int, RGMAException **);

/** Frees a Consumer */
PRIVATE void freeResource(RGMAConsumer *r) {

    int i;

    if (r) {
        lib_free(r->url);
        if (r->bsock != NULL) {
            tcp_close(r->bsock);
        }
        lib_free(r->query);
        lib_free(r->queryType);

        for (i = 0; i < r->numProducers; i++) {
            lib_free(r->producers[i]);
        }
        lib_free(r->producers);
        lib_free(r); /* Must be last */
    }
}

/** Called by close and destroy */
PRIVATE void exterminate(RGMAConsumer *r, char *cmd, RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    const char *parameters[20];
    int n;
    STRINGINT connectionId_s;

    *exceptionPP = NULL;

    if (r) {
        n = 0;
        parameters[n++] = "connectionId";
        parameters[n++] = ITOA(connectionId_s, (r)->connectionId);

        rs = sendCommand(&(r->bsock), r->url, cmd, n / 2, parameters, exceptionPP);
        if (rs) {
            lib_freeTupleSet(rs);
            freeResource(r);
        }
    }
}

PRIVATE void restore(RGMAConsumer *r, RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    const char **parameters;
    int n, i;
    STRINGINT queryInterval_s, timeout_s;

    *exceptionPP = NULL;

    parameters = (const char **) malloc((8 + r->numProducers * 2) * sizeof(char *));
    if (parameters == NULL) {
        lib_setOutOfMemoryException(exceptionPP);
        return;
    }

    n = 0;

    parameters[n++] = "select";
    parameters[n++] = r->query;

    parameters[n++] = "queryType";
    parameters[n++] = r->queryType;

    if (r->queryInterval) {

        parameters[n++] = "timeIntervalSec";
        parameters[n++] = ITOA(queryInterval_s, r->queryInterval);
    }

    if (r->timeout) {
        parameters[n++] = "timeoutSec";
        parameters[n++] = ITOA(timeout_s, r->timeout);
    }

    for (i = 0; i < r->numProducers; i++) {
        parameters[n++] = "producerConnections";
        parameters[n++] = r->producers[i];
    }

    rs = sendCommand(&(r->bsock), r->url, "/createConsumer", n / 2, parameters, exceptionPP);
    lib_free(parameters);
    if (*exceptionPP) {
        return;
    }

    r->connectionId = lib_TupleSetToInt(rs, exceptionPP);
}

PUBLIC
void RGMAConsumer_close(RGMAConsumer *r, RGMAException **exceptionPP) {
    exterminate(r, "/close", exceptionPP);
}

PUBLIC
void RGMAConsumer_destroy(RGMAConsumer *r, RGMAException **exceptionPP) {
    exterminate(r, "/destroy", exceptionPP);
}

PUBLIC RGMAConsumer * RGMAConsumer_create(const char* query, RGMAQueryType queryType, int queryInterval, int timeout,
        int numProducers, RGMAResourceEndpoint * producers, RGMAException ** exceptionPP) {

    RGMAConsumer *r;
    STRINGINT id_s;
    char *producerString;
    char *url, *idString;
    int i, id;

    *exceptionPP = NULL;

    /* Use calloc to clear memory so that can free it easily if things go wrong */
    r = (RGMAConsumer *) calloc(1, sizeof(RGMAConsumer));
    if (r == NULL) {
        return NULL; /* Nothing else we can do. */
    }

    r->query = lib_dupString(query, exceptionPP);
    r->timeout = timeout;
    r->queryInterval = queryInterval;
    r->numProducers = numProducers;
    r->producers = calloc(numProducers, sizeof(char *));
    for (i = 0; i < numProducers; i++) {

        id = (producers[i]).resourceId;
        idString = ITOA(id_s, id);
        url = (&producers[i])->url;
        producerString = (char*) malloc(strlen(id_s) + strlen(url) + 2); /* one for the space and one for the trailing 0 */
        strcpy(producerString, idString);
        strcat(producerString, " ");
        strcat(producerString, url);
        r->producers[i] = producerString;
    }

    if (queryType == RGMAQueryType_C) {
        if (queryInterval) {
            lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid query type to have a query interval", 0);
        }
        r->queryType = lib_dupString("continuous", exceptionPP);
    } else if (queryType == RGMAQueryType_H) {
        if (queryInterval) {
            lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid query type to have a query interval", 0);
        }
        r->queryType = lib_dupString("history", exceptionPP);
    } else if (queryType == RGMAQueryType_L) {
        if (queryInterval) {
            lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid query type to have a query interval", 0);
        }
        r->queryType = lib_dupString("latest", exceptionPP);
    } else if (queryType == RGMAQueryType_S) {
        if (queryInterval) {
            lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid query type to have a query interval", 0);
        }
        r->queryType = lib_dupString("static", exceptionPP);
    } else if (queryType == RGMAQueryTypeWithInterval_C) {
        if (!queryInterval) {
            lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid query type to have a query interval", 0);
        }
        r->queryType = lib_dupString("continuous", exceptionPP);
    } else if (queryType == RGMAQueryTypeWithInterval_H) {
        if (!queryInterval) {
            lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid query type to have a query interval", 0);
        }
        r->queryType = lib_dupString("history", exceptionPP);
    } else if (queryType == RGMAQueryTypeWithInterval_L) {
        if (!queryInterval) {
            lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid query type to have a query interval", 0);
        }
        r->queryType = lib_dupString("latest", exceptionPP);
    }

    r->url = lib_getServiceURL("ConsumerServlet", exceptionPP);
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

PUBLIC void RGMAConsumer_abort(RGMAConsumer *r, RGMAException **exceptionPP) {

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAConsumer pointer is NULL", 0);
        return;
    }

    *exceptionPP = NULL;

    doAbort(r, exceptionPP);
    if (*exceptionPP) {
        if ((*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            restore(r, exceptionPP);
            if (*exceptionPP) {
                return;
            }
            doAbort(r, exceptionPP);
            if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
                (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
            }
        }
    }
}

PRIVATE void doAbort(RGMAConsumer *r, RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    STRINGINT connectionId_s;
    const char *parameters[20];
    int n;

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, r->connectionId);

    rs = sendCommand(&(r->bsock), r->url, "/abort", n / 2, parameters, exceptionPP);
    if (*exceptionPP) {
        return;
    }
    lib_freeTupleSet(rs);
}

PUBLIC int RGMAConsumer_hasAborted(RGMAConsumer *r, RGMAException **exceptionPP) {

    int result;

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAConsumer pointer is NULL", 0);
        return 0;
    }

    *exceptionPP = 0;

    result = doHasAborted(r, exceptionPP);
    if (*exceptionPP) {
        if ((*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            restore(r, exceptionPP);
            if (*exceptionPP) {
                return 0;
            }
            result = doHasAborted(r, exceptionPP);
            if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
                (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
            }
        }
    }

    if (*exceptionPP) {
        return 0;
    }
    return result;
}

PRIVATE int doHasAborted(RGMAConsumer *r, RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    STRINGINT connectionId_s;
    const char *parameters[20];
    int n, result;

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, r->connectionId);

    rs = sendCommand(&(r->bsock), r->url, "/hasAborted", n / 2, parameters, exceptionPP);

    if (*exceptionPP) {
        return 0;
    }

    result = lib_TupleSetToBoolean(rs, exceptionPP);
    if (*exceptionPP) {
        lib_freeTupleSet(rs);
        return 0;
    }

    return result;
}

PUBLIC RGMATupleSet * RGMAConsumer_pop(RGMAConsumer *r, int maxCount, RGMAException **exceptionPP) {

    RGMATupleSet * result;

    if (!r) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "RGMAConsumer pointer is NULL", 0);
        return NULL;
    }

    *exceptionPP = NULL;

    result = doPop(r, maxCount, exceptionPP);
    if (*exceptionPP) {
        if ((*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
            restore(r, exceptionPP);
            if (*exceptionPP) {
                return NULL;
            }
            result = doPop(r, maxCount, exceptionPP);
            if (*exceptionPP && (*exceptionPP)->type == RGMA_UNKNOWNRESOURCEEXCEPTION) {
                (*exceptionPP)->type = RGMAExceptionType_TEMPORARY;
                return NULL;
            }
            if (*exceptionPP) { /* Temporary or permanent */
                return NULL;
            }
            lib_free(result->warning); /* It has recovered */
            result->warning = lib_dupString("The query was restarted - many duplicates may be returned.", exceptionPP);
            if (*exceptionPP) {
                lib_freeTupleSet(result);
                return NULL;
            }
        }
    }
    return result;
}

PRIVATE RGMATupleSet * doPop(RGMAConsumer *r, int maxCount, RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    STRINGINT connectionId_s, maxCount_s;
    const char *parameters[20];
    int n;

    n = 0;
    parameters[n++] = "connectionId";
    parameters[n++] = ITOA(connectionId_s, r->connectionId);
    parameters[n++] = "maxCount";
    parameters[n++] = ITOA(maxCount_s, maxCount);

    rs = sendCommand(&(r->bsock), r->url, "/pop", n / 2, parameters, exceptionPP);
    if (*exceptionPP) {
        return NULL;
    }
    if (r->eof) {
        lib_free(rs->warning);
        rs->warning = lib_dupString("You have called pop again after end of results returned.", exceptionPP);
        if (*exceptionPP) {
            lib_freeTupleSet(rs);
            return NULL;
        }
    }
    r->eof = rs->isEndOfResults;
    return rs;
}

