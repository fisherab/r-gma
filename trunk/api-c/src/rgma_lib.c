/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/* This file contains functions to manage common RGMA objects (and the
 rgma.conf properties file). It is shared by all implementations. */

#include <stdio.h>  /* for NULL, FILE, fopen, fclose, fgets, sprintf */
#include <string.h> /* for strcmp, strlen, strcpy */
#include <stdlib.h> /* for malloc, free, getenv */
#include <ctype.h>  /* for isspace etc */

#include "rgma_lib.h"

#define PRIVATE static
#define PUBLIC  /* empty */

/* Properties used by lib_getServiceURL(). */

#define PROPS_MAXFILE 255
#define PROPS_MAXLINE 255

/* Line separator. */

#define COLUMN_SEPARATOR "|"
#define LINE_SEPARATOR "-------------------------------------------------------------------------------\n"

/* Convenience macro to check whether column type is valid. */

#define ValidColumn(t) \
        ((t) == RGMAColumnType_INTEGER || \
         (t) == RGMAColumnType_REAL || \
         (t) == RGMAColumnType_DOUBLE || \
         (t) == RGMAColumnType_TIMESTAMP || \
         (t) == RGMAColumnType_CHAR || \
         (t) == RGMAColumnType_VARCHAR || \
         (t) == RGMAColumnType_DATE || \
         (t) == RGMAColumnType_TIME)

#define ERROR_ValueNotInTupleSet "Requested value was not returned by the servlet."

/* PUBLIC FUNCTIONS ***********************************************************/

PUBLIC char *lib_dupString(const char* in, RGMAException ** exceptionPP) {
    char* out;
    out = (char *) malloc(strlen(in) + 1);
    if (!out) {
        lib_setOutOfMemoryException(exceptionPP);
        return NULL;
    }
    strcpy(out, in);
    return out;
}

/**
 * Local function to fill in an RGMAException object. The parameters provided are
 * used, and new memory is allocated for a copy of the message.
 *
 * @param  target            Exception to set.
 * @param  type         Exception type.
 * @param  errorMessage      Error message.
 * @param  numSuccessfulOps  Number of ops.
 *
 * @return Nothing
 */

PUBLIC void lib_setException(RGMAException **PPexception, int type, char *message, int numSuccessfulOps) {
    RGMAException* Pexception;

    Pexception = (RGMAException *) malloc(sizeof(RGMAException));
    if (!Pexception) {
        return; /* Unable to report the error */
    }
    *PPexception = Pexception;

    Pexception->type = type;
    Pexception->message = (char *) malloc(strlen(message) + 1);
    strcpy(Pexception->message, message);
    Pexception->numSuccessfulOps = numSuccessfulOps;
}

/**
 * Local function to fill in an RGMAException object for an out of memory error.
 *
 * @param  target            Exception to set.
 *
 * @return Nothing
 */

PUBLIC void lib_setOutOfMemoryException(RGMAException **target) {
    lib_setException(target, RGMAExceptionType_PERMANENT, "No more free memory available", 0);
}


/**
 * Frees a result set: checks for NULL pointers to that even a partially
 * constructed result set can be freed.
 *
 * @param  rs        Result set to free.
 *
 * @return NULL (for convenience).
 */
PUBLIC void *lib_freeTupleSet(RGMATupleSet *rs) {
    int i, j;

    if (rs) {
        if (rs->tuples) {
            for (i = 0; i < rs->numTuples; ++i) {
                if (rs->tuples[i].cols) {
                    for (j = 0; j < rs->numCols; ++j) {
                        lib_free(rs->tuples[i].cols[j]);
                    }

                    lib_free(rs->tuples[i].cols);
                }
            }
            lib_free(rs->tuples);
        }

        lib_free(rs->warning);
        lib_free(rs);
    }

    return NULL;
}

/**
 * Converts a (single-valued) result set into an integer value.
 *
 * @param  rs             Result set to read.
 * @param  exceptionPP     Exception to write on error.
 *
 * @return value
 */

PUBLIC int lib_TupleSetToInt(RGMATupleSet *rs, RGMAException **exceptionPP) {
    char *value;
    int ivalue;

    value = lib_TupleSetValue(rs, 0, 0, exceptionPP);
    if (*exceptionPP) {
        lib_freeTupleSet(rs);
        return 0;
    }

    ivalue = atoi(value);
    lib_freeTupleSet(rs);
    return ivalue;
}

/**
 * Converts a (single-valued) result set into a boolean value.
 *
 * @param  rs             Result set to read.
 * @param  exceptionPP     Exception to write on error.
 *
 * @return value
 */

PUBLIC int lib_TupleSetToBoolean(RGMATupleSet *rs, RGMAException **exceptionPP) {
    char *value;
    int bvalue;

    value = lib_TupleSetValue(rs, 0, 0, exceptionPP);
    if (*exceptionPP) {
        lib_freeTupleSet(rs);
        return 0;
    }

    bvalue = lib_checkBoolean(value);
    lib_freeTupleSet(rs);
    return bvalue;
}

/**
 * Converts a (single-valued) result set into a string value.
 *
 * @param  rs             Result set to read.
 * @param  exceptionPP     Exception to write on error.
 *
 * @return value
 */

PUBLIC char* lib_TupleSetToString(RGMATupleSet *rs, RGMAException **exceptionPP) {
    char *value;
    char *cvalue;

    value = lib_TupleSetValue(rs, 0, 0, exceptionPP);
    if (*exceptionPP) {
        lib_freeTupleSet(rs);
        return NULL;
    }

    cvalue = (char *) malloc(strlen(value) + 1);
    strcpy(cvalue, value);
    lib_freeTupleSet(rs);
    return cvalue;
}

/**
 * Function to free up an RGMAResourceEndpoint structure.
 *
 * @param  endpointP       Endpoint to free up.
 *
 * @return Nothing.
 */

PUBLIC void lib_freeEndpoint(RGMAResourceEndpoint *endpointP) {
    if (endpointP != NULL) {
        lib_free(endpointP->url);
        lib_free(endpointP);
    }
}

PUBLIC void lib_freeException(RGMAException *e) {
    lib_free(e->message);
    lib_free(e);
}

PUBLIC int lib_TupleSetToStringList(RGMATupleSet *rs, RGMAStringList **stringListPP, RGMAException **exceptionPP) {
    RGMAStringList *slist;
    char *value;
    int i;

    slist = (RGMAStringList *) malloc(sizeof(RGMAStringList));
    if (slist == NULL) {
        lib_freeTupleSet(rs);
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
        return 0;
    }

    slist->numStrings = 0;
    slist->string = NULL;

    if (rs == NULL || rs->numTuples == 0) {
        lib_freeTupleSet(rs);
        *stringListPP = slist;
        return 1; /* successful but empty */
    }

    slist->numStrings = rs->numTuples;
    slist->string = (char **) calloc(slist->numStrings, sizeof(char *));
    if (slist->string == NULL) {
        lib_freeTupleSet(rs);
        lib_freeStringList(slist);
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
        return 0;
    }

    for (i = 0; i < slist->numStrings; ++i) {
        value = lib_TupleSetValue(rs, i, 0, exceptionPP);
        if (*exceptionPP) {
            lib_freeTupleSet(rs);
            lib_freeStringList(slist);
            return 0;
        }

        slist->string[i] = (char *) malloc(strlen(value) + 1);
        strcpy(slist->string[i], value);
    }

    lib_freeTupleSet(rs);
    *stringListPP = slist;
    return 1;
}

/**
 * Function to free up an RGMAStringList.
 *
 * @param  stringListPP        Pointer to RGMAStringList to free.
 *
 * @return Nothing.
 */

PUBLIC void lib_freeStringList(RGMAStringList *stringListP) {
    int i, numStrings;
    char **string;

    if (stringListP != NULL) {
        string = stringListP->string;
        numStrings = stringListP->numStrings;
        if (string) {
            for (i = 0; i < numStrings; ++i) {
                lib_free(string[i]);
            }
            lib_free(string);
        }
        lib_free(stringListP);
    }
}

/**
 * Converts a result set to a tuple store list.
 *
 * @param  rs               Result set to read.
 * @param  tupleStoreListPP  Tuple store list to write.
 * @param  exceptionP       Exception to write on error.
 *
 * @return 1 on success or 0 on failure.
 */

PUBLIC int lib_TupleSetToTupleStoreList(RGMATupleSet *rs, RGMATupleStoreList **tupleStoreListPP,
        RGMAException **exceptionPP) {
    RGMATupleStoreList *ts;
    char *logicalName, *isHistory, *isLatest;
    int i;

    ts = (RGMATupleStoreList *) malloc(sizeof(RGMATupleStoreList));
    if (ts == NULL) {
        lib_freeTupleSet(rs);
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
        return 0;
    }

    ts->numTupleStores = 0;
    ts->tupleStore = NULL;

    if (rs == NULL || rs->numTuples == 0) {
        lib_freeTupleSet(rs);
        *tupleStoreListPP = ts;
        return 1; /* successful but empty */
    }

    ts->numTupleStores = rs->numTuples;
    ts->tupleStore = (RGMATupleStore *) calloc(ts->numTupleStores, sizeof(RGMATupleStore));
    if (ts->tupleStore == NULL) {
        lib_freeTupleSet(rs);
        lib_freeTupleStoreList(ts);
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
        return 0;
    }

    for (i = 0; i < ts->numTupleStores; ++i) {
        logicalName = lib_TupleSetValue(rs, i, 0, exceptionPP);
        isHistory = lib_TupleSetValue(rs, i, 1, exceptionPP);
        isLatest = lib_TupleSetValue(rs, i, 2, exceptionPP);
        if (*exceptionPP) {
            lib_freeTupleSet(rs);
            lib_freeTupleStoreList(ts);
            return 0;
        }
        ts->tupleStore[i].logicalName = (char *) malloc(strlen(logicalName) + 1);
        strcpy(ts->tupleStore[i].logicalName, logicalName);
        ts->tupleStore[i].isHistory = lib_checkBoolean(isHistory);
        ts->tupleStore[i].isLatest = lib_checkBoolean(isLatest);
    }

    lib_freeTupleSet(rs);
    *tupleStoreListPP = ts;
    return 1;
}

/**
 * Function to free up an RGMATupleStoreList.
 *
 * @param  tupleStoreListP         Pointer to RGMATupleStoreList to free.
 *
 * @return Nothing.
 */

PUBLIC void lib_freeTupleStoreList(RGMATupleStoreList *tupleStoreListP) {
    int i, numTupleStores;
    RGMATupleStore *tupleStore;

    if (tupleStoreListP != NULL) {
        tupleStore = tupleStoreListP->tupleStore;
        numTupleStores = tupleStoreListP->numTupleStores;
        if (tupleStore) {
            for (i = 0; i < numTupleStores; ++i) {
                lib_free(tupleStore[i].logicalName);
            }
            lib_free(tupleStore);
        }
        lib_free(tupleStoreListP);
    }
}

PUBLIC char *lib_TupleSetValue(RGMATupleSet *rs, int rowNum, int colNum, RGMAException **exceptionPP) {
    /* Sanity checks (user may have altered the result set). */
    if (rs == NULL) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "TupleSet pointer is null", 0);
        return NULL;
    }

    if (rowNum < 0 || rowNum >= rs->numTuples || colNum < 0 || colNum >= rs->numCols) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Attempt to read outside result set", 0);
        return NULL;
    }

    if (rs->tuples == NULL || rs->tuples[rowNum].cols == NULL) {
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "TupleSet has been corrupted", 0);
        return NULL;
    }
    return rs->tuples[rowNum].cols[colNum];
}

/**
 * Converts a result set to an RGMAProducerTableEntryList.
 *
 * @param  rs                        Result set to read.
 * @param  producerTableEntryListPP  RGMAProducerTableEntryList to write.
 * @param  exceptionP                Exception to write on error.
 *
 * @return 1 on success or 0 on failure.
 */
PUBLIC int lib_TupleSetToProducerTableEntryList(RGMATupleSet *rs,
        RGMAProducerTableEntryList **producerTableEntryListPP, RGMAException **exceptionPP) {
    RGMAProducerTableEntryList *ptelist;
    RGMAResourceEndpoint *endpoint;
    char *url, *connectionId, *predicate, *isHistory, *isLatest, *isContinuous, *isStatic, *isSecondary, *hrpSec;
    int i;

    ptelist = (RGMAProducerTableEntryList *) malloc(sizeof(RGMAProducerTableEntryList));
    if (ptelist == NULL) {
        lib_freeTupleSet(rs);
        lib_setOutOfMemoryException(exceptionPP);
        return 0;
    }

    ptelist->numProducerTableEntries = 0;
    ptelist->producerTableEntry = NULL;

    if (rs == NULL || rs->numTuples == 0) {
        lib_freeTupleSet(rs);
        *producerTableEntryListPP = ptelist;
        return 1; /* successful but empty */
    }

    ptelist->numProducerTableEntries = rs->numTuples;
    ptelist->producerTableEntry = (RGMAProducerTableEntry *) calloc(ptelist->numProducerTableEntries,
            sizeof(RGMAProducerTableEntry));
    if (ptelist->producerTableEntry == NULL) {
        lib_freeTupleSet(rs);
        lib_freeProducerTableEntryList(ptelist);
        lib_setOutOfMemoryException(exceptionPP);
        return 0;
    }

    for (i = 0; i < ptelist->numProducerTableEntries; ++i) {
        url = lib_TupleSetValue(rs, i, 0, exceptionPP);
        connectionId = lib_TupleSetValue(rs, i, 1, exceptionPP);
        isSecondary = lib_TupleSetValue(rs, i, 2, exceptionPP);
        isContinuous = lib_TupleSetValue(rs, i, 3, exceptionPP);
        isStatic = lib_TupleSetValue(rs, i, 4, exceptionPP);
        isHistory = lib_TupleSetValue(rs, i, 5, exceptionPP);
        isLatest = lib_TupleSetValue(rs, i, 6, exceptionPP);
        predicate = lib_TupleSetValue(rs, i, 7, exceptionPP);
        hrpSec = lib_TupleSetValue(rs, i, 8, exceptionPP);

        if (*exceptionPP) {
            lib_freeTupleSet(rs);
            lib_freeProducerTableEntryList(ptelist);
            return 0;
        }

        endpoint = (RGMAResourceEndpoint *) malloc(sizeof(RGMAResourceEndpoint));
        if (endpoint == NULL) {
            lib_freeTupleSet(rs);
            lib_freeProducerTableEntryList(ptelist);
            lib_setOutOfMemoryException(exceptionPP);
            return 0;
        }
        endpoint->resourceId = atoi(connectionId);
        endpoint->url = (char*) malloc(strlen(url) + 1);
        strcpy(endpoint->url, url);
        ptelist->producerTableEntry[i].endpoint = endpoint;
        ptelist->producerTableEntry[i].isSecondary = lib_checkBoolean(isSecondary);
        ptelist->producerTableEntry[i].isContinuous = lib_checkBoolean(isContinuous);
        ptelist->producerTableEntry[i].isStatic = lib_checkBoolean(isStatic);
        ptelist->producerTableEntry[i].isHistory = lib_checkBoolean(isHistory);
        ptelist->producerTableEntry[i].isLatest = lib_checkBoolean(isLatest);
        ptelist->producerTableEntry[i].predicate = (char *) malloc(strlen(predicate) + 1);
        strcpy(ptelist->producerTableEntry[i].predicate, predicate);
        ptelist->producerTableEntry[i].hrpSec = atoi(hrpSec);
    }

    lib_freeTupleSet(rs);
    *producerTableEntryListPP = ptelist;
    return 1;
}

/**
 * Function to free up an RGMAProducerTableEntryList.
 *
 * @param  producerTableEntryListP        Pointer to RGMAProducerTableEntryList
 *                                        to free.
 *
 * @return Nothing.
 */

PUBLIC void lib_freeProducerTableEntryList(RGMAProducerTableEntryList *producerTableEntryListP) {
    int i, numProducerTableEntries;
    RGMAProducerTableEntry *producerTableEntry;

    if (producerTableEntryListP != NULL) {
        producerTableEntry = producerTableEntryListP->producerTableEntry;
        numProducerTableEntries = producerTableEntryListP->numProducerTableEntries;
        if (producerTableEntry) {
            for (i = 0; i < numProducerTableEntries; ++i) {
                lib_freeEndpoint(producerTableEntry[i].endpoint);
                lib_free(producerTableEntry[i].predicate);
            }
            lib_free(producerTableEntry);
        }
        lib_free(producerTableEntryListP);
    }
}

/**
 * Converts a result set to an RGMAIndexList.
 *
 * NB. This is a management function which doesn't have to be hugely efficient,
 * so the linear searches through the result set ought to be OK.
 *
 * @param  rs                 Result set to read.
 * @param  indexListPP        RGMAIndexList to write.
 * @param  exceptionP         Exception to write on error.
 *
 * @return 1 on success or 0 on failure.
 */

PUBLIC int lib_TupleSetToIndexList(RGMATupleSet *rs, RGMAIndexList **indexListPP, RGMAException **exceptionPP) {
    RGMAIndexList *indexList;
    char *indexName, *columnName;
    int i, j;

    indexList = (RGMAIndexList *) malloc(sizeof(RGMAIndexList));
    if (indexList == NULL) {
        lib_freeTupleSet(rs);
        lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "No free memory left", 0);
        return 0;
    }

    indexList->numIndexes = 0;
    indexList->index = NULL;

    if (rs == NULL || rs->numTuples == 0) {
        lib_freeTupleSet(rs);
        *indexListPP = indexList;
        return 1; /* successful but empty */
    }

    /* Rows need to be grouped by indexName before we can count the
     number of indexes and the number of columns in each. */

    for (i = 0; i < rs->numTuples; ++i) {
        indexName = lib_TupleSetValue(rs, i, 0, exceptionPP);
        columnName = lib_TupleSetValue(rs, i, 1, exceptionPP);

        if (*exceptionPP) {
            lib_freeTupleSet(rs);
            lib_freeIndexList(indexList);
            return 0;
        }

        for (j = 0; j < indexList->numIndexes; ++j) {
            if (strcmp(indexName, indexList->index[j].indexName) == 0) {
                ++indexList->index[j].numCols;
                break;
            }
        }

        if (j == indexList->numIndexes) /* not found */
        {
            ++indexList->numIndexes;

            indexList->index = (RGMAIndex *) realloc(indexList->index, indexList->numIndexes * sizeof(RGMAIndex));
            if (indexList->index == NULL) {
                lib_freeTupleSet(rs);
                lib_freeIndexList(indexList);
                lib_setOutOfMemoryException(exceptionPP);
                return 0;
            }

            indexList->index[j].indexName = lib_dupString(indexName, exceptionPP);
            indexList->index[j].numCols = 1;
            indexList->index[j].column = NULL; /* updated below */
        }

        indexList->index[j].column = (char **) realloc(indexList->index[j].column, indexList->index[j].numCols
                * sizeof(char *));
        if (indexList->index[j].column == NULL) {
            lib_freeTupleSet(rs);
            lib_freeIndexList(indexList);
            lib_setOutOfMemoryException(exceptionPP);
            return 0;
        }
        indexList->index[j].column[indexList->index[j].numCols - 1] = lib_dupString(columnName, exceptionPP);
    }

    lib_freeTupleSet(rs);
    if (*exceptionPP) {
        lib_freeIndexList(indexList);
        return 0;
    }

    *indexListPP = indexList;
    return 1;
}

/**
 * Function to free up an RGMAIndexList.
 *
 * @param  indexListP         Pointer to RGMAIndexList to free.
 *
 * @return Nothing.
 */

PUBLIC void lib_freeIndexList(RGMAIndexList *indexListP) {
    int i, j, numIndexes, numCols;
    RGMAIndex *index;
    char **column;

    if (indexListP != NULL) {
        index = indexListP->index;
        numIndexes = indexListP->numIndexes;
        if (index) {
            for (i = 0; i < numIndexes; ++i) {
                lib_free(index[i].indexName);
                column = index[i].column;
                numCols = index[i].numCols;
                if (column) {
                    for (j = 0; j < numCols; ++j) {
                        lib_free(column[j]);
                    }
                    lib_free(column);
                }
            }
            lib_free(index);
        }
        lib_free(indexListP);
    }
}

/**
 * Converts a result set to an RGMATableDefinition.
 *
 * @param  rs                 Result set to read.
 * @param  tableDefinitionPP  RGMATableDefinition to write.
 * @param  exceptionP         Exception to write on error.
 *
 * @return 1 on success or 0 on failure.
 */

PUBLIC int lib_TupleSetToTableDefinition(RGMATupleSet *rs, RGMATableDefinition **tableDefinitionPP,
        RGMAException **exceptionPP) {
    RGMATableDefinition *tdef;
    char *tableName, *viewFor, *columnName, *type, *size, *notNull, *primaryKey;
    int i;

    tdef = (RGMATableDefinition *) malloc(sizeof(RGMATableDefinition));
    if (tdef == NULL) {
        lib_freeTupleSet(rs);
        lib_setOutOfMemoryException(exceptionPP);
        return 0;
    }

    tableName = lib_TupleSetValue(rs, 0, 0, exceptionPP);

    tdef->tableName = lib_dupString(tableName, exceptionPP);

    viewFor = lib_TupleSetValue(rs, 0, 6, exceptionPP);
    if (viewFor) {
        tdef->viewFor = lib_dupString(viewFor, exceptionPP);
    } else {
        tdef->viewFor = NULL;
    }

    tdef->numColumns = rs->numTuples;
    tdef->column = (RGMAColumnDefinition *) calloc(tdef->numColumns, sizeof(RGMAColumnDefinition));
    if (tdef->column == NULL) {
        lib_setOutOfMemoryException(exceptionPP);
    }

    if (*exceptionPP) {
        lib_freeTupleSet(rs);
        lib_freeTableDefinition(tdef);
        return 0;
    }

    for (i = 0; i < tdef->numColumns; ++i) {
        columnName = lib_TupleSetValue(rs, i, 1, exceptionPP);
        type = lib_TupleSetValue(rs, i, 2, exceptionPP);
        size = lib_TupleSetValue(rs, i, 3, exceptionPP);
        notNull = lib_TupleSetValue(rs, i, 4, exceptionPP);
        primaryKey = lib_TupleSetValue(rs, i, 5, exceptionPP);

        if (*exceptionPP) {
            lib_freeTupleSet(rs);
            lib_freeTableDefinition(tdef);
            return 0;
        }

        tdef->column[i].name = lib_dupString(columnName, exceptionPP);
        if (strcmp(type, "INTEGER") == 0) {
            tdef->column[i].type = RGMAColumnType_INTEGER;
        } else if (strcmp(type, "REAL") == 0) {
            tdef->column[i].type = RGMAColumnType_REAL;
        } else if (strcmp(type, "DOUBLE") == 0) {
            tdef->column[i].type = RGMAColumnType_DOUBLE;
        } else if (strcmp(type, "CHAR") == 0) {
            tdef->column[i].type = RGMAColumnType_CHAR;
        } else if (strcmp(type, "VARCHAR") == 0) {
            tdef->column[i].type = RGMAColumnType_VARCHAR;
        } else if (strcmp(type, "TIMESTAMP") == 0) {
            tdef->column[i].type = RGMAColumnType_TIMESTAMP;
        } else if (strcmp(type, "DATE") == 0) {
            tdef->column[i].type = RGMAColumnType_DATE;
        } else if (strcmp(type, "TIME") == 0) {
            tdef->column[i].type = RGMAColumnType_TIME;
        } else {
            lib_setException(exceptionPP, RGMAExceptionType_PERMANENT, "Invalid column type in XML from server", 0);
        }
        tdef->column[i].size = atoi(size);
        tdef->column[i].notNull = lib_checkBoolean(notNull);
        tdef->column[i].primaryKey = lib_checkBoolean(primaryKey);
    }

    lib_freeTupleSet(rs);
    if (*exceptionPP) {
        lib_freeTableDefinition(tdef);
        return 0;
    }

    *tableDefinitionPP = tdef;
    return 1;
}

/**
 * Function to free up an RGMATableDefinition.
 *
 * @param  tableDefinitionP         Pointer to RGMATableDefinition to free.
 *
 * @return Nothing.
 */

PUBLIC void lib_freeTableDefinition(RGMATableDefinition *tableDefinitionP) {
    int numColumns, i;
    RGMAColumnDefinition *column;

    if (tableDefinitionP) {
        lib_free(tableDefinitionP->tableName);
        lib_free(tableDefinitionP->viewFor);
        column = tableDefinitionP->column;
        numColumns = tableDefinitionP->numColumns;
        if (column) {
            for (i = 0; i < numColumns; ++i) {
                lib_free(column[i].name);
            }
            lib_free(column);
        }
        lib_free(tableDefinitionP);
    }
}

/**
 * Function to extract a service URL from the R-GMA properties file.
 * The caller should free the memory returned.
 *
 * @param  service        The service required (e.g. RGMAService_CONSUMER).
 * @param  exceptionP     Exception written on error.
 *
 * @return Pointer to requested URL, or NULL on any error.
 */

PUBLIC char *lib_getServiceURL(const char* serviceName, RGMAException **exceptionP) {
    char *props_dir, props_file[PROPS_MAXFILE], *url, *hostname, *port, *prefix;
    int urllen;

    static char* commonUrl = NULL; /* Holds the string common to all URLs - only computed once */

    url = NULL;

    if (commonUrl == NULL) {
        props_dir = getenv("RGMA_HOME");
        if (props_dir != NULL) {
            sprintf(props_file, "%s/%s", props_dir, "etc/rgma/rgma.conf");
            hostname = lib_getProperty(props_file, "hostname");
            port = lib_getProperty(props_file, "port");
            prefix = lib_getProperty(props_file, "prefix");
            if (prefix == NULL) {
                prefix = "R-GMA";
            }
            if (hostname != NULL && port != NULL) {
                urllen = strlen("https://") + strlen(hostname) + strlen(":") + strlen(port) + strlen("/") + strlen(
                        prefix) + strlen("/");
                commonUrl = (char *) malloc(urllen + 1);
                if (commonUrl != NULL) {
                    strcpy(commonUrl, "https://");
                    strcat(commonUrl, hostname);
                    strcat(commonUrl, ":");
                    strcat(commonUrl, port);
                    strcat(commonUrl, "/");
                    strcat(commonUrl, prefix);
                    strcat(commonUrl, "/");
                    lib_free(hostname);
                    lib_free(port);
                }
            }
        }
    }

    if (commonUrl != NULL) {
        urllen = strlen(commonUrl) + strlen(serviceName);
        url = (char *) malloc(urllen + 1);
        if (url != NULL) {
            strcpy(url, commonUrl);
            strcat(url, serviceName);
        }
    }

    if (url == NULL) {
        lib_setException(exceptionP, RGMAExceptionType_PERMANENT, "Can't get service URL from rgma.conf file", 0);
    }

    return url;
}

/**
 * Function to extract a property from a properties file.
 * The C++ implementation (Properties.cpp) checks for escaped new-line
 * characters, but this is never used, so we don't bother here. The caller
 * should free the memory returned.
 *
 * @param  props_file      The properties file name.
 * @param  property        The property required.
 *
 * @return Pointer to value of requested property, or NULL on any error.
 */

PUBLIC
char *lib_getProperty(char *props_file, const char *property) {
    FILE *fp;
    char buf[PROPS_MAXLINE], *s, *t, *value;

    /* Open the file. */

    fp = fopen(props_file, "r");
    if (fp == NULL)
        return NULL;

    /* Loop through each line in turn, cleaning it up and looking for
     the requested property. */

    while (fgets(buf, sizeof(buf), fp)) {
        /* Remove comments and white space, check for the key/value
         separator and the end-of-line. */

        value = NULL;
        t = buf;

        for (s = buf; *s != '\0'; ++s) {
            if (*s == '#' || *s == '!')
                *s = '\n'; /* comment */
            if (*s == '\n')
                break;
            else if (isspace((int)*s))
                continue;
            else if ((*s == ':' || *s == '=') && value == NULL) {
                *t++ = '\0';
                value = t;
            } else
                *t++ = *s;
        }
        if (*s != '\n') /* missing end of line => buffer overflow */
        {
            fclose(fp);
            return NULL;
        }
        *t = '\0'; /* terminate the value string */

        /* See if the key matches the requested property. */

        if (strcmp(property, buf) == 0) {
            s = (char *) malloc(strlen(value) + 1);
            if (s != NULL)
                strcpy(s, value);
            fclose(fp);
            return s;
        }
    }

    fclose(fp);
    return NULL; /* not found */
}

/* Convenience function to check if a string, when mapped to upper case,
 equates to "TRUE". */

PUBLIC
int lib_checkBoolean(const char *string) {
    int i;
    char copy[5];

    if (string != NULL && strlen(string) == 4) {
        for (i = 0; i <= 4; ++i)
            copy[i] = toupper((int)string[i]);
        if (strcmp(copy, "TRUE") == 0)
            return 1;
    }
    return 0;
}

/**
 * Convenience function to check if a value is NULL before freeing it.
 */

PUBLIC void lib_free(void *mem) {
    if (mem != NULL)
        free(mem);
}

/* End of file. */
