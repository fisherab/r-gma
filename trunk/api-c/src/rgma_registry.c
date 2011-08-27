/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API - Registry.
 */

#include <stdio.h>   /* for NULL and FILE */
#include <string.h>  /* for strcmp */
#include <stdlib.h>  /* for malloc, free */

#include "rgma.h"
#include "rgma_command.h"
#include "rgma_lib.h"
#include "rgma_private.h"

PUBLIC RGMAProducerTableEntryList *RGMARegistry_getAllProducersForTable(const char *vdbName, const char *tableName,
        RGMAException **exceptionPP) {

    RGMAProducerTableEntryList *tableEntryList;
    RGMATupleSet *rs;
    const char *parameters[20];
    char *url;
    int n;

    *exceptionPP = NULL;

    url = lib_getServiceURL("RegistryServlet", exceptionPP);
    if (*exceptionPP) {
        return NULL;
    }

    n = 0;
    parameters[n++] = "vdbName";
    parameters[n++] = vdbName;
    parameters[n++] = "canForward";
    parameters[n++] = "true"; /* always true from user API */
    parameters[n++] = "tableName";
    parameters[n++] = tableName;

    rs = sendCommand(NULL, url, "/getAllProducersForTable", n / 2, parameters, exceptionPP);
    lib_free(url);
    if (rs == NULL){
        return NULL;
    }

    if (!lib_TupleSetToProducerTableEntryList(rs, &tableEntryList, exceptionPP)) {
        return NULL;
    }

    return tableEntryList;
}
