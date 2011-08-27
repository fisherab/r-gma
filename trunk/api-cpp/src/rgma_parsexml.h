/*
 *  Copyright (c) 2004 on behalf of the EU EGEE Project:
 *  The European Organization for Nuclear Research (CERN),
 *  Istituto Nazionale di Fisica Nucleare (INFN), Italy
 *  Datamat Spa, Italy
 *  Centre National de la Recherche Scientifique (CNRS), France
 *  CS Systeme d'Information (CSSI), France
 *  Royal Institute of Technology, Center for Parallel Computers (KTH-PDC), Sweden
 *  Universiteit van Amsterdam (UvA), Netherlands
 *  University of Helsinki (UH.HIP), Finland
 *  University of Bergen (UiB), Norway
 *  Council for the Central Laboratory of the Research Councils (CCLRC), United Kingdom
 */

/* Header file for rgma_parsexml.c. */
#ifndef RGMA_PARSEXML_H
#define RGMA_PARSEXML_H
#ifdef __cplusplus
extern "C" {
#endif
/* Typedefs and public functions for parsexml library. */

typedef struct ATTRIBUTE_S ATTRIBUTE;
struct ATTRIBUTE_S
{
	char		*key;
	char		*value;
	ATTRIBUTE	*next;
};

typedef struct ELEMENT_S ELEMENT;
struct ELEMENT_S
{
	char		*name;
	char		*data;
	ATTRIBUTE	*atts;
	int		closed;
	ELEMENT		*parent;
	ELEMENT		*children;
	ELEMENT		*next;
};

extern ELEMENT *parsexml_parse(char *);
extern void parsexml_free(ELEMENT *);
extern ELEMENT *getElementByName(ELEMENT *first, const char *name);
extern char *getAttributeByName(ELEMENT *first, const char *name);
#ifdef __cplusplus
 }
#endif
#endif /* RGMA_PARSEXML_H */
