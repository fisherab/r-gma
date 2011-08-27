/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import org.glite.rgma.server.system.QueryProperties;

/**
 * Exception thrown when a cursor is opened specifying a query
 * type that is not supported by the tuple store.
 */
@SuppressWarnings("serial")
public class QueryTypeNotSupportedException extends Exception {
    /**
     * Creates a new QueryTypeNotSupportedException.
     *
     * @param queryProps Request query type.
     */
    public QueryTypeNotSupportedException(QueryProperties queryProps) {
        super("Query type not supported: " + queryProps);
    }
}
