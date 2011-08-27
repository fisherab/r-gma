/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.naming.ConfigurationException;

import org.glite.rgma.server.services.sql.CreateIndexStatement;
import org.glite.rgma.server.services.sql.CreateTableStatement;
import org.glite.rgma.server.services.sql.InsertStatement;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.UpdateStatement;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSetWithLastTUID;

/**
 * Provides methods for database access.
 */
public interface TupleStoreDatabase {

	/**
	 * Closes the specified database cursor. If the cursor ID does not exist, no action is taken.
	 * 
	 * @param cursorID
	 *            Cursor identifier.
	 * @throws RGMAPermanentException
	 * @see #openCursor(SelectStatement)
	 */
	void closeCursor(int cursorID) throws RGMAPermanentException;

	/**
	 * Returns the number of tuples stored in the specified table.
	 * 
	 * @param tableName
	 *            Name of table.
	 * @return The number of valid tuples in the specified table or <code>0</code> if the table is not present.
	 * @throws SQLException
	 *             If the DBMS can not be accessed.
	 * @throws RGMAPermanentException
	 */
	int count(String tableName) throws RGMAPermanentException;

	String createTable(String ownerDN, String logicalName, String vdbTableName, String tableType, CreateTableStatement cts) throws RGMAPermanentException;

	/**
	 * Deletes all tuples older than maxAgeSecs that have not been streamed to all known consumers. Calculates cut-off
	 * time as (currentTimeMillis() - maxAgeSecs * 1000) and executes DELETE statement. The lastReadTUID input is used
	 * to ensure that we do not remove tuples that have not been streamed to all consumers. The lastReadTUID may be -1
	 * if there are no consumers or > 0 to indicate the last tupleID streamed to all known consumers. It will not be
	 * called if has the value 0
	 * 
	 * @throws RGMAPermanentException
	 * @see org.glite.rgma.server.services.database.TupleStoreDatabase#delete(java.lang.String, int)
	 */
	int deleteByHRP(String tableName, int maxAgeSecs, int lastReadTUID) throws RGMAPermanentException;

	/**
	 * Deletes all tuples which exceeds LRP secs from the given table
	 * 
	 * @param tableName
	 * @throws TableNotFoundException
	 * @throws SQLException
	 * @throws SQLException
	 * @throws RGMAPermanentException
	 */
	int deleteByLRP(String tableName) throws RGMAPermanentException;

	/**
	 * Removes a tuple store from the list. Note that the <code>ownerDN</code> is not used for authorization, but just
	 * to uniquely identify the logical name.
	 * 
	 * @param ownerDN
	 *            DN of tuple store owner.
	 * @param logicalName
	 *            Logical name of tuple store to remove.
	 * @throws RGMAPermanentException
	 */
	void dropTupleStore(String ownerDN, String logicalName) throws RGMAPermanentException;

	/**
	 * Fetches at most <code>maxRows</code> rows from the database using the specified cursor.
	 * 
	 * @param cursorID
	 *            Cursor to use.
	 * @param maxRows
	 *            Maximum number of rows to return.
	 * @return At most <code>maxRows</code> results from the query.
	 * @throws CursorNotFoundException
	 *             If the ID does not reference a valid cursor.
	 * @throws SQLException
	 *             If the RDBMS could not be contacted.
	 * @throws RGMAPermanentException
	 */
	TupleSetWithLastTUID fetch(int cursorID, int maxRows) throws RGMAPermanentException;

	/**
	 * Gets the ID of the first tuple in the given table that has an RgmaInsertTime greater than or equal to
	 * <code>startTimeMS</code>.
	 * 
	 * @param tableName
	 *            Name of table.
	 * @param startTimeMS
	 *            Start time (in millis).
	 * @return The ID of the first tuple in the given table that has an RgmaInsertTime greater than or equal to
	 *         <code>startTimeMS</code>.
	 * @throws SQLException
	 *             If the database can't be accessed to retrieve the tuple ID.
	 * @throws RGMAPermanentException
	 */
	int findFirstTupleID(String tableName, long startTimeMS) throws RGMAPermanentException;

	int getMaxTUID(String physicalTableName) throws RGMAPermanentException;

	/**
	 * Executes an INSERT on this database.
	 * 
	 * @param insertStatement
	 *            SQL INSERT statement.
	 * @throws DuplicateKeyException
	 *             If an entry with the same primary key already exists in the database.
	 * @throws SQLException
	 *             If the RDBMS could not be contacted or the INSERT fails.
	 * @throws RGMAPermanentException
	 */
	void insert(InsertStatement insertStatement) throws RGMAPermanentException;

	/**
	 * Gets a list of permanent tuple stores.
	 * 
	 * @param userDN
	 *            DN of user making call.
	 * @return A list of details of permanent tuple stores.
	 * @throws RGMAPermanentException
	 * @throws TupleStoreException
	 *             If the tuple store list can not be retrieved.
	 * @throws ConfigurationException
	 */
	List<TupleStoreDetails> listTupleStores(String userDN) throws RGMAPermanentException;

	/**
	 * Opens a cursor to allow results to be fetched in chunks. Guarantees that results won't change for the lifetime of
	 * the cursor.
	 */
	int openCursor(SelectStatement selectStatement) throws RGMAPermanentException, NumericException;

	/**
	 * Executes a SELECT query on this database.
	 * 
	 * @param selectStatement
	 *            SQL SELECT query.
	 * @return The results from the query as a ResultSet.
	 * @throws SQLException
	 *             If the RDBMS could not be contacted or the SELECT fails.
	 * @throws RGMAPermanentException
	 * @see #openCursor(SelectStatement)
	 */
	TupleSetWithLastTUID select(SelectStatement selectStatement) throws RGMAPermanentException;

	/**
	 * Executes an UPDATE on this database.
	 * 
	 * @param updateStatement
	 *            SQL UPDATE statement.
	 * @return Number of rows updated (could be 0).
	 * @throws SQLException
	 *             If the RDBMS could not be contacted or the UPDATE fails.
	 * @throws RGMAPermanentException
	 */
	int update(UpdateStatement updateStatement) throws RGMAPermanentException;

	Map<ResourceEndpoint, Integer> getConsumerTUIDs(String physicalTableName) throws RGMAPermanentException;

	void storeConsumerTUIDs(String physicalTableName, Map<ResourceEndpoint, Integer> consumerTUID) throws RGMAPermanentException;

	void closeTupleStore(List<String> physicalTableNames, boolean permanent) throws RGMAPermanentException;

	TupleSetWithLastTUID getContinuous(SelectStatement select, int maxCount) throws RGMAPermanentException;

	void shutdown() throws RGMAPermanentException;

	void createIndex(CreateIndexStatement cis) throws RGMAPermanentException;
}
