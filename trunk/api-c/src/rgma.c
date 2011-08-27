/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains the functions which form the public interface to the
 R-GMA C API.
 */

#include <stdio.h>   /* for NULL and FILE */
#include <string.h>  /* for strcmp */
#include <stdlib.h>  /* for malloc, free */

#include "rgma.h"
#include "rgma_command.h"
#include "rgma_lib.h"
#include "rgma_private.h"

PUBLIC void RGMA_freeEndpoint(RGMAResourceEndpoint *endpointP) {
    lib_freeEndpoint(endpointP);
}

PUBLIC void RGMA_freeException(RGMAException *exceptionP) {
    if (exceptionP != NULL) {
        lib_freeException(exceptionP);
    }
}

PUBLIC void RGMA_freeStringList(RGMAStringList *stringListP) {
    lib_freeStringList(stringListP);
}

PUBLIC void RGMA_freeTupleSet(RGMATupleSet *rs) {
    lib_freeTupleSet(rs);
}

PUBLIC void RGMA_freeTupleStoreList(RGMATupleStoreList *tupleStoreListP) {
    lib_freeTupleStoreList(tupleStoreListP);
}

PUBLIC void RGMA_freeProducerTableEntryList(RGMAProducerTableEntryList *producerTableEntryListP) {
    lib_freeProducerTableEntryList(producerTableEntryListP);
}

PUBLIC void RGMA_freeIndexList(RGMAIndexList *indexListP) {
    lib_freeIndexList(indexListP);
}

PUBLIC void RGMA_freeTableDefinition(RGMATableDefinition *tableDefinitionP) {
    lib_freeTableDefinition(tableDefinitionP);
}

