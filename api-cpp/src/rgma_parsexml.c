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

/* This is a naive not-especially-validating XML parser, designed primarily to
   handle an R-GMA XMLResponse document. It has the following limitations:

   - It does not handle DOCTYPE and CDATA sections, comments or imports.
   - It does not substitute external or character entities (but does substitute
     the internal ones: &amp;, &apos;, &gt;, &lt; and &quot;).
   - It does not support mixed content in elements (apart from white space
     around sub-elements, which is discarded).
   - It is non-validating, but it will never crash, and the tree it creates (if
     any) can always be safely passed to free_elements(), but its contents
     will be unreliable if the input data is invalid.

   The parser creates an element-tree, pointing into the original data buffer.
   New memory is grabbed for the element and attribute nodes, but the "name",
   "data", "key" and "value" members point into the original data buffer, so
   you should not dispose of the buffer until you have finished with the tree.
   The input buffer is extensively modified by this parser. All memory
   allocated by the parser is freed in the event of an error.
*/

#include <stdio.h>  /* for printf() (not essential) and NULL */
#include <stdlib.h> /* for malloc() and free() */
#include <string.h> /* only for strcmp() (not essential) */
#include <ctype.h>  /* only for isspace() (not essential) */

#include "rgma_parsexml.h"

#define PRIVATE static
#define PUBLIC  /* empty */

#define TRUE 1
#define FALSE 0

#define STATE_DOCUMENT 0
#define STATE_TAGNAME 1
#define STATE_PI 2
#define STATE_TAG 3
#define STATE_ATTNAME 4
#define STATE_ATTSEPARATOR 5
#define STATE_ATTVALUE 6
#define STATE_ELEMENTDATA 7

/* Private function prototypes. */

PRIVATE ELEMENT *start_element(ELEMENT *, char *, char *);
PRIVATE ELEMENT *end_element(ELEMENT *, char *, char *);
PRIVATE ELEMENT *set_element_data(ELEMENT *, char *, char *);
PRIVATE ELEMENT *add_attribute(ELEMENT *, char *, char *);
PRIVATE ELEMENT *set_attribute_data(ELEMENT *, char *, char *);
PRIVATE void free_elements(ELEMENT *);
PRIVATE void decode_entities(char *);

/* PUBLIC FUNCTIONS ***********************************************************/

/**
 * Parses buffer, creating element tree pointing into buffer. Element tree
 * should be freed up by calling parsexml_free() when it is no longer required.
 * User buffer (buf) should only be freed after the tree is disposed of.
 *
 * @param  buf      0 terminated buffer to parse (will be modified by this parser)
 *
 * @return Head of element tree, or NULL on memory/parse error.
 */

PUBLIC ELEMENT *parsexml_parse(char *buf)
{
	ELEMENT *e, *root;
	char	*token,
		*s, *t,
		*tag_name;
	int	i,
		state,
		end_tag,
		quote_character,
		last_c;

	/* Initialise. */

	state = STATE_DOCUMENT;
	last_c = EOF;
	root = e = NULL;
	t = buf;

	token = NULL;             /* to shut the compiler up */
	tag_name = NULL;          /* to shut the compiler up */
	end_tag = FALSE;          /* to shut the compiler up */
	quote_character = '\0';   /* to shut the compiler up */

	/* Process each input character in turn, and decide what to do with it
	   according to the current state. Characters are copied from "s" to
	   "t" (possibly with modification). Both point into the input data
	   buffer, and "t" always lags behind "s", so can always be safely
	   written to. */

	int bufsize = strlen(buf);
	for (i = 0; i < bufsize; ++i)
	{
		s = &(buf[i]);

		switch (state)
		{
			case STATE_DOCUMENT:

			if (isspace(*s))
			{
				break; /* ignore white space */
			}
			else if (*s == '<')
			{
				token = t;
				tag_name = NULL;
				end_tag = FALSE;
				state = STATE_TAGNAME;
			}
			else /* invalid character */
			{
			    free_elements(root);
				return (NULL);
			}
			break;

			case STATE_TAGNAME:

			if (isspace(*s))
			{
				if (!end_tag)
				{
					tag_name = token;
					e = start_element(e, tag_name, t++);
					if (!e) {
					    free_elements(root);
					    return (NULL);
					}
					if (root == NULL) root = e;
				}

				state = STATE_TAG;
			}
			else if (*s == '?')
			{
				state = STATE_PI;
			}
			else if (*s == '!')
			{
				/* <!DOCTYPE, <!CDATA or <!--  not supported */
			    free_elements(root);
				return (NULL);
			}
			else if (*s == '/')
			{
				end_tag = TRUE;
			}
			else if (*s == '>')
			{
				if (!end_tag) /* start tag */
				{
					if (!tag_name) tag_name = token;
					e = start_element(e, tag_name, t++);
					if (!e) {
					    free_elements(root);
					    return (NULL);
					}
					if (root == NULL) root = e;

					token = t;
					state = STATE_ELEMENTDATA;
				}
				else if (last_c == '/') /* start & end tag */
				{
					if (!tag_name) tag_name = token;
					e = start_element(e, tag_name, t);
					if (!e) {
					    free_elements(root);
					    return (NULL);
					}
					if (root == NULL) root = e;

					e = end_element(e, tag_name, t++);
					if (!e) {
					    free_elements(root);
					    return (NULL);
					}

					state = STATE_DOCUMENT;
				}
				else /* end tag */
				{
					if (!tag_name) tag_name = token;
					e = end_element(e, tag_name, t++);
					if (!e) {
					    free_elements(root);
					    return (NULL);
					}

					state = STATE_DOCUMENT;
				}
			}
			else /* if valid TAGNAME character */
			{
				*t++ = *s;
			}
			break;

			case STATE_PI:

			if (*s == '>' && last_c == '?')
			{
				state = STATE_DOCUMENT;
			}
			break;

			case STATE_TAG:

			if (isspace(*s))
			{
				break; /* ignore white space */
			}
			else if (*s == '/')
			{
				end_tag = TRUE;
			}
			else if (*s == '>')
			{
				/* Let STATE_TAGNAME take the strain... */
				--i; /* push back */
				state = STATE_TAGNAME;
			}
			else /* if valid ATTNAME character */
			{
				token = t;
				*t++ = *s;
				state = STATE_ATTNAME;
			}
			break;

			case STATE_ATTNAME:

			if (isspace(*s) || *s == '=')
			{
				e = add_attribute(e, token, t++);
				if (!e) {
				    free_elements(root);
				    return (NULL);
				}
				state = STATE_ATTSEPARATOR;
			}
			else /* if valid ATTNAME character */
			{
				*t++ = *s;
			}
			break;

			case STATE_ATTSEPARATOR:

			if (isspace(*s) || *s == '=')
			{
				break; /* ignore white space */
			}
			else if (*s == '"' || *s == '\'')
			{
				token = t;
				quote_character = *s;
				state = STATE_ATTVALUE;
			}
			else
			{
			    free_elements(root);
				return (NULL);
			}
			break;

			case STATE_ATTVALUE:

			if (*s == quote_character)
			{
				e = set_attribute_data(e, token, t++);
				if (!e) {
				    free_elements(root);
				    return (NULL);
				}

				state = STATE_TAG;
			}
			else /* if valid ATTVALUE character */
			{
				*t++ = *s;
			}
			break;

			case STATE_ELEMENTDATA:

			if (*s == '<')
			{
				e = set_element_data(e, token, t++);
				if (!e) {
				    free_elements(root);
				    return (NULL);
				}

				token = t;
				tag_name = NULL;
				state = STATE_TAGNAME;
			}
			else /* if valid ELEMENTDATA character */
			{
				*t++ = *s;
			}
			break;
		}

		last_c = buf[i];
	}

	/* If the input was valid, the element stack should have unwound
	   properly. */

	if (root != NULL && (root != e || root->closed == FALSE))
	{
	    free_elements(root);
		return (NULL);
	}

	return root;
}


/**
 * Frees up element tree returned by parsexml_parse().
 *
 * @param  root  root of list to be freed
 *
 * @return Nothing
 */

PUBLIC void parsexml_free(ELEMENT *root)
{
	free_elements(root);
}

/* PRIVATE FUNCTIONS **********************************************************/

/**
 * Creates a new element node with the given name and adds it to the element
 * tree.
 *
 * @param  e           pointer to parent element
 * @param  name        element name (may not be NULL)
 * @param  terminator  pointer to terminating '\0' character of name
 *
 * @return Pointer to the new element or NULL on error.
 */

PRIVATE ELEMENT *start_element(ELEMENT *e, char *name, char *terminator)
{
	ELEMENT *new; /* new element */

	if (name == NULL) return NULL; /* self defence */

	new = (ELEMENT *) malloc(sizeof(ELEMENT));
	if (new == NULL) return NULL;

	if (e != NULL && e->name == name) { /* don't add it twice */
	    free(new);
	    return e;
	}

	*terminator = '\0';
	new->name = name;
	new->data = terminator; /* default to '\0' */
	new->atts = NULL;
	new->closed = FALSE;
	if (e == NULL)
	{
		new->parent = NULL;
	}
	else if (e->closed == FALSE)
	{
		e->children = new;
		if (e->data) e->data[0] =  '\0'; /* scrub any element data */
		new->parent = e;
	}
	else
	{
		e->next = new;
		new->parent = e->parent;
	}
	new->children = NULL;
	new->next = NULL;

	return new;
}

/**
 * Called in response to an end-tag: finds the corresponding start-tag in the
 * tree and marks it as closed.
 *
 * @param  e           pointer to element to close (or one of its child nodes)
 * @param  name        element name (may not be NULL)
 * @param  terminator  pointer to terminating '\0' character of name
 *
 * @return Pointer to the element changed or NULL on error.
 */

PRIVATE ELEMENT *end_element(ELEMENT *e, char *name, char *terminator)
{
	if (e == NULL || name == NULL) return NULL; /* self defence */

	/* Find the corresponding start tag. */

	if (e->closed == TRUE) e = e->parent;
	if (e == NULL) return NULL; /* end tag without start tag */
	e->closed = TRUE;

	/* Validation check that the start and end tag names match. */

	*terminator = '\0';
	if (e->name != NULL && strcmp(name, e->name) != 0) return NULL;

	return e;
}

/**
 * Adds character data to an existing element.
 *
 * @param  e           pointer to element to which to add data
 * @param  data        element data
 * @param  terminator  pointer to terminating '\0' character of data
 *
 * @return Pointer to the element changed or NULL on error.
 */

PRIVATE ELEMENT *set_element_data(ELEMENT *e, char *data, char *terminator)
{
	if (e == NULL) return NULL; /* self defence */

	*terminator = '\0';
	decode_entities(data);
	e->data = data;

	return e;
}

/**
 * Adds a new attribute node to an existing element.
 *
 * @param  e           pointer to element to which to add attribute
 * @param  key         attribute key
 * @param  terminator  pointer to terminating '\0' character of key
 *
 * @return Pointer to the element changed or NULL on error.
 */

PRIVATE ELEMENT *add_attribute(ELEMENT *e, char *key, char *terminator)
{
	ATTRIBUTE *new, *a;  /* new attribute and pointer into list */

	if (e == NULL) return NULL; /* self defence */

	new = (ATTRIBUTE *) malloc(sizeof(ATTRIBUTE));
	if (new == NULL) return NULL;

	*terminator = '\0';
	new->key = key;
	new->value = terminator; /* default to '\0' */
	new->next = NULL;

	if (e->atts == NULL) e->atts = new;
	else
	{
		for (a = e->atts; a->next != NULL; a = a->next); /* last one */
		a->next = new;
	}

	return e;
}

/**
 * Sets a value for an existing attribute node.
 *
 * @param  e           pointer to element to which to attribute is attached
 * @param  value         attribute value
 * @param  terminator  pointer to terminating '\0' character of value
 *
 * @return Pointer to the element changed or NULL on error.
 */

PRIVATE ELEMENT *set_attribute_data(ELEMENT *e, char *data, char *terminator)
{
	ATTRIBUTE *a;  /* pointer into attribute list */

	if (e == NULL || e->atts == NULL) return NULL; /* self defence */

	*terminator = '\0';
	decode_entities(data);
	for (a = e->atts; a->next != NULL; a = a->next); /* find last one */
	a->value = data;

	return e;
}

/**
 * Frees up an entire element tree: even incomplete trees can be freed safely,
 * so the function is called internally before returning a parse error.
 *
 * @param  root  pointer to root element of tree.
 */

PRIVATE void free_elements(ELEMENT *root)
{
	ELEMENT *e, *next_e;    /* pointers into element tree */
	ATTRIBUTE *a, *next_a;  /* pointers into attribute list */

	for (e = root; e != NULL; e = next_e)
	{
		for (a = e->atts; a != NULL; a = next_a)
		{
			next_a = a->next;
			free(a);
		}

		if (e->children) free_elements(e->children); /* recursive */

		next_e = e->next;
		free(e);
	}
}


/**
 * Decodes entities (&apos;/&amp;/&gt;/&lt;/&quot;), replacing them with
 * the corresponding ASCII character. Can be re-configured for streaming,
 * if required.
 *
 * @param  buf  pointer to buffer to process (in situ)
 *
 * @return Nothing
 */

PRIVATE void decode_entities(char *buf)
{
	char *s, *t;  /* pointers into buf ("from" and "to" respectively) */

	t = buf;
	for (s = buf; *s != '\0'; ++s)
	{
		if (s[0] == '&' && s[1] == 'a' && s[2] == 'm' &&
		    s[3] == 'p' && s[4] == ';')
		{
			*t++ = '&';
			s += 4;
		}
		else if (s[0] == '&' && s[1] == 'a' && s[2] == 'p' &&
		         s[3] == 'o' && s[4] == 's' && s[5] == ';')
		{
			*t++ = '\'';
			s += 5;
		}
		else if (s[0] == '&' && s[1] == 'g' && s[2] == 't' &&
		         s[3] == ';')
		{
			*t++ = '>';
			s += 3;
		}
		else if (s[0] == '&' && s[1] == 'l' && s[2] == 't' &&
		         s[3] == ';')
		{
			*t++ = '<';
			s += 3;
		}
		else if (s[0] == '&' && s[1] == 'q' && s[2] == 'u' &&
		         s[3] == 'o' && s[4] == 't' && s[5] == ';')
		{
			*t++ = '"';
			s += 5;
		}
		else *t++ = *s;
	}

	*t = '\0';
}

/* Searches "first" and its immediate siblings for the next occurrence of
 an element with name "name". Returns NULL if none found. */
PUBLIC ELEMENT *getElementByName(ELEMENT *first, const char *name) {
    ELEMENT *e;
    for (e = first; e != NULL; e = e->next) {
        if (strcmp(e->name, name) == 0)
            return e;
    }

    return NULL; /* not found */
}

/* Searches all attributes of element for one with name "name".
 Returns value of attribute or it's not found. */
PUBLIC char *getAttributeByName(ELEMENT *first, const char *name) {
    ATTRIBUTE *a;
    for (a = first->atts; a != NULL; a = a->next) {
        if (strcmp(a->key, name) == 0)
            return a->value;
    }

    return NULL; /* not found */
}
