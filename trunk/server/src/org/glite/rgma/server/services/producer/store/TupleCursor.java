/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.TupleSetWithLastTUID;

/**
 * Cursor used to retrieve tuples from a tuple store to answer queries.
 * 
 * @see TupleStore#openCursor(SelectStatement, QueryProperties, long)
 */
public interface TupleCursor {
	/**
	 * Pops tuples from this cursor.
	 * 
	 * @param maxCount
	 *            Maximum number of tuples to retrieve.
	 * 
	 * @return At most <code>cursorID</code> tuples from the specified cursor.
	 * 
	 * @throws RGMAPermanentException
	 *             If the tuple store can not be accessed.
	 */
	TupleSetWithLastTUID pop(int maxCount) throws RGMAPermanentException;

	/**
	 * Closes this cursor and frees up any resources used by it.
	 * 
	 * @throws RGMAPermanentException
	 * 
	 * @throws TupleStoreException
	 *             If the cursor can't be closed.
	 */
	void close() throws RGMAPermanentException;
}
