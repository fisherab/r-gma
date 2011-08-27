/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API - Service.
 */

#include <stdio.h>   /* for NULL and FILE */
#include <string.h>  /* for strcmp */
#include <stdlib.h>  /* for malloc, free */

#include "rgma.h"
#include "rgma_command.h"
#include "rgma_lib.h"
#include "rgma_private.h"

PUBLIC char *RGMA_getVersion(RGMAException **exceptionPP) {

    char *version;
    RGMATupleSet *rs;
    char *url;

    *exceptionPP = NULL;

    url = lib_getServiceURL("RGMAService", exceptionPP);
    if (url == NULL) {
        return NULL;
    }

    rs = sendCommand(NULL, url, "/getVersion", 0, NULL, exceptionPP);
    lib_free(url);
    if (rs == NULL) {
        return NULL;
    }

    version = lib_TupleSetToString(rs, exceptionPP);
    if (*exceptionPP) {
        return NULL;
    }

    return version;
}

PUBLIC void RGMA_dropTupleStore(const char *logicalName, RGMAException **exceptionPP) {

    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("RGMAService", exceptionPP);
    if (url == NULL) {
        return;
    }

    n = 0;
    parameters[n++] = "logicalName";
    parameters[n++] = logicalName;

    rs = sendCommand(NULL, url, "/dropTupleStore", n / 2, parameters, exceptionPP);
    lib_free(url);

    if (rs == NULL) {
        return;
    }
    lib_freeTupleSet(rs);
    return;
}

PUBLIC RGMATupleStoreList *RGMA_listTupleStores(RGMAException **exceptionPP) {

    *exceptionPP = NULL;

    RGMATupleStoreList *ts;
    RGMATupleSet *rs;
    char *url;

    url = lib_getServiceURL("RGMAService", exceptionPP);
    if (url == NULL) {
        return NULL;
    }

    rs = sendCommand(NULL, url, "/listTupleStores", 0, NULL, exceptionPP);
    lib_free(url);

    if (rs == NULL) {
        return NULL;
    }

    if (!lib_TupleSetToTupleStoreList(rs, &ts, exceptionPP)) {
        return NULL;
    }

    return ts;
}

PUBLIC int RGMA_getTerminationInterval(RGMAException **exceptionPP) {
    RGMATupleSet *rs;
    char *url;

    *exceptionPP = NULL;

    url = lib_getServiceURL("RGMAService", exceptionPP);
    if (url == NULL) {
        return -1;
    }

    rs = sendCommand(NULL, url, "/getTerminationInterval", 0, NULL, exceptionPP);
    lib_free(url);

    if (*exceptionPP) {
        return -1;
    }

    return lib_TupleSetToInt(rs, exceptionPP);
}
