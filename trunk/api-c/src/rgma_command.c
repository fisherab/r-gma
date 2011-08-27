/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
#include <stdio.h>  /* for NULL */
#include <string.h> /* for strlen, strcmp, strcpy */
#include <stdlib.h> /* for malloc, free */
#include <ctype.h>  /* for toupper */
#include "rgma_command.h"
#include "rgma_tcp.h" /* for BUFFERED_SOCKET */
#include "rgma_http.h"
#include "rgma_parsexml.h"
#include "rgma_lib.h"
#define PRIVATE static
#define PUBLIC /* empty */
/* Private function prototypes. */
PRIVATE RGMATupleSet *extract_TupleSet(ELEMENT *, RGMAException **);
PRIVATE RGMATupleSet *extract_OldTupleSet(ELEMENT *, RGMAException **);
PRIVATE void extract_exception(ELEMENT *, RGMAException **, char);
PRIVATE void extract_OldException(ELEMENT *, RGMAException **);
PRIVATE ELEMENT *getElementByName(ELEMENT *, const char *);
PRIVATE char *getAttributeByName(ELEMENT *, const char *);

/* PUBLIC FUNCTIONS ***********************************************************/

/**
 * Function to send a command (HTTP GET request) to a remote servlet,
 * and return an RGMATupleSet or an RGMAException explaining what went wrong.
 *
 * @param  bsockP                Pointer to buffered socket.
 * @param  url                   Servlet URL
 * @param  operation             R-GMA operation (e.g. "/declareTable")
 * @param  num_parameters        Number of parameters for operation
 * @param  parameters            Array of parameter name/value pairs.
 *
 * @return Pointer to RGMATupleSet or NULL on error.
 */

PUBLIC RGMATupleSet *sendCommand(BUFFERED_SOCKET **bsockP, char *url, char *operation, int num_parameters,
        const char **parameters, RGMAException **exceptionP) {
    RGMATupleSet *rs;
    ELEMENT *root, *e;
    int status;
    char *response;
    char *erm = NULL;

    /* Send HTTP request to R-GMA servlet. */
    status = http_get(bsockP, url, operation, num_parameters, parameters, &response, &erm);
    if (status != 0) {
        if (status == 1) {
            lib_setException(exceptionP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
        } else if (status == 2) {
            lib_setException(exceptionP, RGMAExceptionType_TEMPORARY, "Unable to connect with R-GMA server", 0);
        } else if (status == 5) {
            lib_setException(exceptionP, RGMAExceptionType_TEMPORARY, "Socket timeout", 0);
        } else if (status == 4) {
            if (erm) {
                lib_setException(exceptionP, RGMAExceptionType_TEMPORARY, erm, 0);
                free(erm);
            } else {
                lib_setException(exceptionP, RGMAExceptionType_TEMPORARY, "Authentication problem", 0);
            }
        } else {
            lib_setException(exceptionP, RGMAExceptionType_TEMPORARY, "Http problem", 0);
        }
        return NULL;
    }

    /* Parse HTTP response. This should be an XMLResponse object. */
    root = parsexml_parse(response, strlen(response));
    if (root == NULL) {
        free(response);
        lib_setException(exceptionP, RGMAExceptionType_TEMPORARY, "Bad XML returned by the R-GMA server - no root", 0);
        return NULL;
    }

    /* Extract either an RGMATupleSet or an RGMAException */
    if (getElementByName(root, "r")) {
        rs = extract_TupleSet(root, exceptionP);
    } else if (getElementByName(root, "t")) {
        extract_exception(root, exceptionP, 't');
    } else if (getElementByName(root, "p")) {
        extract_exception(root, exceptionP, 'p');
    } else if (getElementByName(root, "u")) {
        extract_exception(root, exceptionP, 'u');
    } else {
        rs = NULL;
        lib_setException(exceptionP, RGMAExceptionType_TEMPORARY,
                "Bad XML returned by the R-GMA server - neither response nor exception", 0);
    }

    parsexml_free(root);
    free(response);
    return rs;
}

/* PRIVATE FUNCTIONS **********************************************************/

PRIVATE RGMATupleSet *extract_TupleSet(ELEMENT *root, RGMAException **exceptionP) {

    /* Create and initialise an empty result set (this can safely be
     passed to lib_freeTupleSet in any state of completeness). */
    RGMATupleSet *rs;
    rs = (RGMATupleSet *) calloc(1, sizeof(RGMATupleSet));
    if (rs == NULL) {
        lib_setOutOfMemoryException(exceptionP);
        return NULL;
    }

    /* Deal with the warning message (if any). */
    char* warning;
    if ((warning = getAttributeByName(root, "m"))) {
        rs->warning = lib_dupString(warning, exceptionP);
    } else {
        rs->warning = lib_dupString("", exceptionP);
    }
    if (*exceptionP) {
        return lib_freeTupleSet(rs);
    }

    char* value;
    value = getAttributeByName(root, "c");
    rs->numCols = value ? atoi(value) : 1;

    value = getAttributeByName(root, "r");
    rs->numTuples = value ? atoi(value) : 1;

    /* Store row values (if any). */
    if (rs->numTuples > 0) {
        rs->tuples = (RGMATuple *) calloc(rs->numTuples, sizeof(RGMATuple));
        if (rs->tuples == NULL) {
            lib_setException(exceptionP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
            return lib_freeTupleSet(rs);
        }
    }

    int i = 0;
    int j = 0;
    ELEMENT* ven;
    for (ven = root->children; ven != NULL; ven = ven->next) {
        if (ven->name[0] == 'e') {
            rs->isEndOfResults = 1;
        } else {
            if (j == 0) {
                if (i == rs->numTuples) {
                    lib_setException(exceptionP, RGMAExceptionType_PERMANENT,
                            "More data returned in XML tuple set than specified in header", 0);
                    return lib_freeTupleSet(rs);
                }
                rs->tuples[i].cols = (char **) calloc(rs->numCols, sizeof(char *));
                if (rs->tuples[i].cols == NULL) {
                    lib_setException(exceptionP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
                    return lib_freeTupleSet(rs);
                }
            }

            if (ven->name[0] == 'v') {
                value = ven->data ? ven->data : "";
                rs->tuples[i].cols[j] = (char *) malloc(strlen(value) + 1);
                if (rs->tuples[i].cols[j] == NULL) {
                    lib_setException(exceptionP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
                    return lib_freeTupleSet(rs);
                } else {
                    strcpy(rs->tuples[i].cols[j], value);
                }
            } else { /* n */
                rs->tuples[i].cols[j] = NULL;
            }

            if (++j == rs->numCols) {
                j = 0;
                i++;
            }
        }
    }

    return rs;
}

/**
 * Local function to extract an RGMAException from an XMLException. Error codes are
 * mapped according to the rules for User APIs.
 *
 * @param  root             <XMLException> element to read.
 * @param  exceptionP       Exception to write.
 *
 * @return Nothing.
 */

PRIVATE void extract_exception(ELEMENT *root, RGMAException **exceptionP, char tpu) {
    if (tpu == 'u') {
        lib_setException(exceptionP, RGMA_UNKNOWNRESOURCEEXCEPTION, "Unknown resource.", 0);
    } else {
        char* message = getAttributeByName(root, "m");
        char* nsoc = getAttributeByName(root, "o");
        int nso = nsoc ? atoi(nsoc) : 0;
        if (tpu == 'p') {
            lib_setException(exceptionP, RGMAExceptionType_PERMANENT, message, nso);
        } else {
            lib_setException(exceptionP, RGMAExceptionType_TEMPORARY, message, nso);
        }
    }
}

/* Searches "first" and its immediate siblings for the next occurrence of
 an element with name "name". Returns NULL if none found. */

PRIVATE ELEMENT *getElementByName(ELEMENT *first, const char *name) {
    ELEMENT *e;
    for (e = first; e != NULL; e = e->next) {
        if (strcmp(e->name, name) == 0)
            return e;
    }

    return NULL; /* not found */
}

/* Searches all attributes of element for one with name "name".
 Returns value of attribute or it's not found. */

PRIVATE char *getAttributeByName(ELEMENT *first, const char *name) {
    ATTRIBUTE *a;
    for (a = first->atts; a != NULL; a = a->next) {
        if (strcmp(a->key, name) == 0)
            return a->value;
    }

    return NULL; /* not found */
}
