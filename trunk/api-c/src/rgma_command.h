/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

#ifndef RGMA_COMMAND_H
#define RGMA_COMMAND_H

#include "rgma.h"     /* for RGMAResultSet and RGMAExceptions */
#include "rgma_tcp.h" /* for BUFFERED_SOCKET */

/* Header file for rgma_command.c. */

static const int RGMA_UNKNOWNRESOURCEEXCEPTION = -1; /* Note that the other exceptions are defined in rgma.h */

extern RGMATupleSet *sendCommand(BUFFERED_SOCKET **, char *, char *, int, const char **, RGMAException**);

#endif /* RGMA_COMMAND_H */
