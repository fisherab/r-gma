/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API - Schema.
 */

#include <stdio.h>   /* for NULL and FILE */
#include <string.h>  /* for strcmp */
#include <stdlib.h>  /* for malloc, free */

#include "rgma.h"
#include "rgma_command.h"
#include "rgma_lib.h"
#include "rgma_private.h"

PUBLIC int RGMASchema_createTable(const char *vdbName, const char *createTableStatement, int numRules, char **rules,
        RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    const char **parameters;
    char *url;
    int n, i;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if ( *exceptionPP) {
        return -1;
    }

    parameters = (const char **) malloc((6 + numRules * 2) * sizeof(char *));
    if (parameters == NULL) {
        lib_free(url);
        lib_setOutOfMemoryException(exceptionPP);
        return -1;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "createTableStatement";
    parameters[n++] = createTableStatement;
    for (i = 0; i < numRules; ++i) {
        parameters[n++] = "tableAuthz";
        parameters[n++] = rules[i];
    }

    rs = sendCommand(NULL, url, "/createTable", n / 2, parameters, exceptionPP);
    lib_free(url);
    lib_free(parameters);

    if (rs == NULL) {
        return -1;
    }
    lib_freeTupleSet(rs);

    return 0;
}

PUBLIC int RGMASchema_dropTable(const char *vdbName, const char *tableName, RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return -1;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "tableName";
    parameters[n++] = tableName;

    rs = sendCommand(NULL, url, "/dropTable", n / 2, parameters, exceptionPP);
    lib_free(url);
    if (rs == NULL) {
        return -1;
    }
    lib_freeTupleSet(rs);

    return 0;
}

PUBLIC int RGMASchema_createIndex(const char *vdbName, const char *createIndexStatement, RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return -1;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "createIndexStatement";
    parameters[n++] = createIndexStatement;

    rs = sendCommand(NULL, url, "/createIndex", n / 2, parameters, exceptionPP);
    free(url);
    if (rs == NULL) {
        return -1;
    }
    lib_freeTupleSet(rs);

    return 0;
}

PUBLIC int RGMASchema_dropIndex(const char *vdbName, const char *tableName, const char *indexName,
        RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return -1;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "indexName";
    parameters[n++] = indexName;
    parameters[n++] = "tableName";
    parameters[n++] = tableName;

    rs = sendCommand(NULL, url, "/dropIndex", n / 2, parameters, exceptionPP);
    free(url);
    if (rs == NULL) {
        return -1;
    }
    lib_freeTupleSet(rs);

    return 0;
}

PUBLIC int RGMASchema_createView(const char *vdbName, const char *createViewStatement, int numRules, char **rules,
        RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    const char **parameters;
    char *url;
    int n, i;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return -1;
    }

    parameters = (const char **) malloc((6 + numRules * 2) * sizeof(char *));
    if (parameters == NULL) {
        free(url);
        lib_setOutOfMemoryException(exceptionPP);
        return -1;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "createViewStatement";
    parameters[n++] = createViewStatement;
    for (i = 0; i < numRules; ++i) {
        parameters[n++] = "viewAuthz";
        parameters[n++] = rules[i];
    }

    rs = sendCommand(NULL, url, "/createView", n / 2, parameters, exceptionPP);
    free(url);
    free(parameters);
    if (rs == NULL) {
        return -1;
    }
    lib_freeTupleSet(rs);

    return 0;
}

PUBLIC int RGMASchema_dropView(const char *vdbName, const char *viewName, RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return -1;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "viewName";
    parameters[n++] = viewName;

    rs = sendCommand(NULL, url, "/dropView", n / 2, parameters, exceptionPP);
    free(url);
    if (rs == NULL) {
        return -1;
    }
    lib_freeTupleSet(rs);

    return 0;
}

PUBLIC RGMAStringList *RGMASchema_getAllTables(const char *vdbName, RGMAException **exceptionPP) {

    RGMAStringList *stringList;
    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return NULL;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */

    rs = sendCommand(NULL, url, "/getAllTables", n / 2, parameters, exceptionPP);
    lib_free(url);
    if (rs == NULL) {
        return NULL;
    }

    if (!lib_TupleSetToStringList(rs, &stringList, exceptionPP)) {
        return NULL;
    }

    return stringList;
}

PUBLIC RGMATableDefinition *RGMASchema_getTableDefinition(const char *vdbName, const char *tableName,
        RGMAException **exceptionPP) {

    RGMATableDefinition *tableDefinition;
    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return NULL;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "tableName";
    parameters[n++] = tableName;

    rs = sendCommand(NULL, url, "/getTableDefinition", n / 2, parameters, exceptionPP);
    free(url);
    if (rs == NULL) {
        return NULL;
    }

    if (!lib_TupleSetToTableDefinition(rs, &tableDefinition, exceptionPP)) {
        return NULL;
    }

    return tableDefinition;
}

PUBLIC RGMAIndexList *RGMASchema_getTableIndexes(const char *vdbName, const char *tableName, RGMAException **exceptionPP) {

    RGMAIndexList *indexList;
    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return NULL;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "tableName";
    parameters[n++] = tableName;

    rs = sendCommand(NULL, url, "/getTableIndexes", n / 2, parameters, exceptionPP);
    free(url);
    if (rs == NULL) {
        return NULL;
    }

    if (!lib_TupleSetToIndexList(rs, &indexList, exceptionPP)) {
        return NULL;
    }

    return indexList;
}

PUBLIC int RGMASchema_setAuthorizationRules(const char *vdbName, const char *tableName, int numRules, char **rules,
        RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    const char **parameters;
    char *url;
    int n, i;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return -1;
    }

    parameters = (const char **) malloc((6 + numRules * 2) * sizeof(char *));
    if (parameters == NULL) {
        lib_free(url);
        lib_setOutOfMemoryException(exceptionPP);
        return -1;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "tableName";
    parameters[n++] = tableName;
    for (i = 0; i < numRules; ++i) {
        parameters[n++] = "tableAuthz";
        parameters[n++] = rules[i];
    }

    rs = sendCommand(NULL, url, "/setAuthorizationRules", n / 2, parameters, exceptionPP);
    lib_free(url);
    free(parameters);
    if (rs == NULL) {
        return -1;
    }
    lib_freeTupleSet(rs);

    return 0;
}

PUBLIC RGMAStringList *RGMASchema_getAuthorizationRules(const char *vdbName, const char *tableName,
        RGMAException **exceptionPP) {

    RGMAStringList *stringList;
    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("SchemaServlet", exceptionPP);
    if (url == NULL) {
        return NULL;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "tableName";
    parameters[n++] = tableName;

    rs = sendCommand(NULL, url, "/getAuthorizationRules", n / 2, parameters, exceptionPP);
    free(url);
    if (rs == NULL) {
        return NULL;}

    if (!lib_TupleSetToStringList(rs, &stringList, exceptionPP)) {
        return NULL;
    }

    return stringList;
}
