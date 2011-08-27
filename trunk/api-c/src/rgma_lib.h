/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

#ifndef RGMA_LIB_H
#define RGMA_LIB_H

/* Header file for rgma_lib.c */

#include <stdio.h> /* for FILE */
#include "rgma.h"  /* for RGMATupleSet, RGMAException and RGMARow */

extern void *lib_freeTupleSet(RGMATupleSet *);
extern void lib_setException(RGMAException **, int, char *, int);
extern void lib_setOutOfMemoryException(RGMAException **);
extern int lib_TupleSetToInt(RGMATupleSet *, RGMAException **);
extern int lib_TupleSetToBoolean(RGMATupleSet *, RGMAException **);
extern char* lib_TupleSetToString(RGMATupleSet *, RGMAException **);
extern void lib_freeEndpoint(RGMAResourceEndpoint *);
extern void lib_freeException(RGMAException *);
extern int lib_TupleSetToStringList(RGMATupleSet *, RGMAStringList **, RGMAException **);
extern void lib_freeStringList(RGMAStringList *);
extern int lib_TupleSetToTupleStoreList(RGMATupleSet *, RGMATupleStoreList **, RGMAException **);
extern void lib_freeTupleStoreList(RGMATupleStoreList *);
extern int lib_TupleSetToIndexList(RGMATupleSet *, RGMAIndexList **, RGMAException **);
extern void lib_freeIndexList(RGMAIndexList *);
extern int lib_TupleSetToTableDefinition(RGMATupleSet *, RGMATableDefinition **, RGMAException **);
extern void lib_freeTableDefinition(RGMATableDefinition *);
extern int lib_TupleSetToProducerTableEntryList(RGMATupleSet *, RGMAProducerTableEntryList **, RGMAException **);
extern void lib_freeProducerTableEntryList(RGMAProducerTableEntryList *);
extern char *lib_getServiceURL(const char *, RGMAException **);
extern int lib_checkBoolean(const char *);
extern void lib_free(void *);
extern char *lib_getProperty(char *, const char *);
extern char *lib_TupleSetValue(RGMATupleSet *, int, int, RGMAException **);
extern char *lib_dupString(const char*, RGMAException **);

#endif /* RGMA_LIB_H */

/* End of file. */
