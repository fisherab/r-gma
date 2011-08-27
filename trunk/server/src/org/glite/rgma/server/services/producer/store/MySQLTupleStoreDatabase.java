/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import javax.naming.ConfigurationException;

import org.glite.rgma.server.services.database.MySQLConnection;
import org.glite.rgma.server.services.sql.ColumnDefinition;
import org.glite.rgma.server.services.sql.CreateIndexStatement;
import org.glite.rgma.server.services.sql.CreateTableStatement;
import org.glite.rgma.server.services.sql.DataType;
import org.glite.rgma.server.services.sql.InsertStatement;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.UpdateStatement;
import org.glite.rgma.server.services.sql.DataType.Type;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.TupleSetWithLastTUID;
import org.glite.rgma.server.system.Storage.StorageType;

/**
 * MySQL implementation of TupleStoreDatabase.
 */
public class MySQLTupleStoreDatabase extends TupleStoreDatabaseBase implements TupleStoreDatabase {

	/**
	 * Database cursor in MySQL, implemented using a DB table.
	 */
	private class MySQLOneTimeCursor {

		/** Index of next tuple to return. */
		private int m_currentIndex;

		/** Cursor ID. */
		private int m_id;

		private int m_numTuples;

		private boolean m_active = true;

		/** SELECT query. */
		private SelectStatement m_selectStatement;

		/** Temporary table name. */
		private String m_tableName;

		/**
		 * Creates a new Cursor.
		 * 
		 * @param id
		 *            Cursor ID.
		 * @param selectStatement
		 *            SELECT query.
		 * @throws SQLException
		 * @throws RGMAPermanentException
		 */
		MySQLOneTimeCursor(int id, SelectStatement selectStatement) throws RGMAPermanentException {
			try {
				m_id = id;
				m_selectStatement = selectStatement;
				m_currentIndex = 0;
				m_tableName = CURSOR_TABLE_PREFIX + m_id;
				m_numTuples = MySQLConnection.executeSimpleUpdate("CREATE TABLE " + m_tableName + " (" + ReservedColumns.RGMA_TUID_ONE_OFF_COLUMN_NAME
						+ " integer auto_increment primary key) " + m_selectStatement);
			} catch (SQLException e) {
				throw new RGMAPermanentException(e);
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Create cursor " + m_tableName + " with " + m_numTuples + "tuples");
			}
		}

		/**
		 * This is to increase the probablity that the DB is cleaned up
		 */
		protected void finalize() throws RGMAPermanentException {
			close();
		}

		/**
		 * Closes this cursor and removes the temporary table.
		 * 
		 * @throws RGMAPermanentException
		 */
		synchronized void close() throws RGMAPermanentException {
			if (m_active) {
				try {
					MySQLConnection.executeSimpleUpdate("DROP TABLE " + m_tableName);
					m_active = false;
				} catch (SQLException e) {
					// Ignore the error
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug("Cursor " + m_tableName + " closed.");
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Cursor " + m_tableName + " already closed.");
				}
			}
		}

		/**
		 * Fetches at most <code>maxRows</code> rows from this cursor.
		 * 
		 * @param maxRows
		 *            Maximum number of rows to return.
		 * @return At most <code>maxRows</code> rows from this cursor.
		 * @throws SQLException
		 * @throws RGMAPermanentException
		 */
		TupleSetWithLastTUID fetch(int maxRows) throws SQLException, RGMAPermanentException {
			int tuplesLeft = m_numTuples - m_currentIndex;
			boolean endOfResults = maxRows >= tuplesLeft;
			maxRows = Math.min(maxRows, tuplesLeft);
			MySQLConnection con = new MySQLConnection();

			try {
				String select = "SELECT * FROM " + m_tableName + " ORDER BY " + ReservedColumns.RGMA_TUID_ONE_OFF_COLUMN_NAME + " LIMIT " + m_currentIndex
						+ ", " + maxRows;
				java.sql.ResultSet resultSet = con.executeQuery(select);
				m_currentIndex += maxRows;
				TupleSetWithLastTUID rs = convertResultSet(resultSet);
				TupleSet ts = rs.getTupleSet();
				ts.setEndOfResults(endOfResults);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Fetched " + ts.size() + " tuples from " + m_tableName);
				}
				return rs;
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
	}

	/**
	 * Translates a CreateTableStatement into a MySQL compatible String.
	 */
	private static String translateCreateTable(CreateTableStatement createTable) {

		StringBuilder buf = new StringBuilder("CREATE TABLE " + createTable.getTableName() + " (");
		boolean firstCol = true;

		for (ColumnDefinition cd : createTable.getColumns()) {
			if (firstCol) {
				firstCol = false;
			} else {
				buf.append(", ");
			}
			buf.append(cd.getName() + " " + cd.getType());
			if (cd.isNotNull()) {
				buf.append(" NOT NULL");
			}
		}
		buf.append(") CHARACTER SET latin1 COLLATE latin1_general_cs");
		return buf.toString();
	}

	/**
	 * Translates the given MySQL type String into a generic R-GMA type. MySQL translates the generic types as follows:
	 * CHAR --> char(1) INTEGER --> int(11) REAL/DOUBLE PRECISION --> double
	 */
	private static DataType translateTypeString(String dataTypeStr) throws SQLException {
		if (dataTypeStr.startsWith("varchar")) {
			int size = getSizeFromType(dataTypeStr);
			return new DataType(Type.VARCHAR, size);
		} else if (dataTypeStr.startsWith("char")) {
			int size = getSizeFromType(dataTypeStr);
			return new DataType(Type.CHAR, size);
		} else if (dataTypeStr.equals("int(11)")) {
			return new DataType(Type.INTEGER, 0);
		} else if (dataTypeStr.equals("double")) {
			return new DataType(Type.DOUBLE_PRECISION, 0);
		} else if (dataTypeStr.equals("date")) {
			return new DataType(Type.DATE, 0);
		} else if (dataTypeStr.equals("time")) {
			return new DataType(Type.TIME, 0);
		} else if (dataTypeStr.equals("timestamp")) {
			return new DataType(Type.TIMESTAMP, 0);
		} else {
			throw new SQLException("Unknown type returned by MySQL database: " + dataTypeStr);
		}
	}

	/** Mapping from cursor ID to Cursor objects. */
	private Map<Integer, MySQLOneTimeCursor> m_cursors;

	private Random m_random;

	public MySQLTupleStoreDatabase() throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			MySQLConnection.init();
			m_currentCursorID = 0;
			m_cursors = new HashMap<Integer, MySQLOneTimeCursor>();
			con = new MySQLConnection();
			java.sql.ResultSet rs = con.executeQuery("SHOW TABLES LIKE 'TupleStore_Mapping'");
			if (!rs.next()) {
				con.executeUpdate("CREATE TABLE TupleStore_Mapping("
						+ "ownerDN text, logicalName text, vdbTableName text, tableType CHAR(1), physicalTableName VARCHAR(255), "
						+ "PRIMARY KEY (physicalTableName))");
				con.executeUpdate("CREATE TABLE TupleStore_TUID("
						+ "physicalTableName VARCHAR(255), consumerURL VARCHAR(255), consumerID INTEGER, TUID INTEGER, "
						+ "PRIMARY KEY(physicalTableName, consumerURL, consumerID))");
				if (LOG.isDebugEnabled()) {
					LOG.debug("Created tuple store metatables.");
				}
			} else {
				java.sql.ResultSet jrs = con.executeQuery("SHOW TABLES LIKE '" + CURSOR_TABLE_PREFIX + "%'");
				List<String> tables = new ArrayList<String>();
				while (jrs.next()) {
					tables.add(jrs.getString(1));
				}
				for (String table : tables) {
					con.executeUpdate("DROP TABLE " + table);
				}
			}
			m_random = new Random();
			if (LOG.isInfoEnabled()) {
				LOG.info("MySQLTupleStoreDatabase ready");
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	/**
	 * Removes the cursor from the list of cursors and closes it.
	 * 
	 * @throws RGMAPermanentException
	 * @see org.glite.rgma.server.services.database.TupleStoreDatabase#closeCursor(int)
	 */
	public void closeCursor(int cursorID) throws RGMAPermanentException {
		MySQLOneTimeCursor cursor = m_cursors.remove(cursorID);
		if (cursor != null) {
			cursor.close();
		} // Do nothing if cursor did not exist.
	}

	public void closeTupleStore(List<String> physicalTableNames, boolean permanent) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			if (!permanent) {
				for (String ptn : physicalTableNames) {
					con.executeUpdate("DROP TABLE " + ptn);
					con.executeUpdate("DELETE FROM TupleStore_TUID WHERE physicalTableName = '" + ptn + "'");
					con.executeUpdate("DELETE FROM TupleStore_Mapping WHERE physicalTableName = '" + ptn + "'");
				}
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	/**
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 * @see TupleStoreDatabase#count(String)
	 */
	public int count(String tableName) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			java.sql.ResultSet countResult = con.executeQuery("SELECT COUNT(*) FROM " + tableName);
			countResult.next();
			return countResult.getInt(1);
		} catch (SQLException e) {
			if (e.getErrorCode() == 1146) {
				LOG.debug("Unable to get count for table " + tableName + " as it no longer exists");
				return 0;
			}
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public void createIndex(CreateIndexStatement cis) throws RGMAPermanentException {
		/*
		 * The index may already exist - in which case must check if index has changed or it may be too long in which
		 * case we need to modify it. First we hope for the best.
		 */
		int MaxColInIndex = 16;
		int MaxBytesInIndex = 1000;

		MySQLConnection con = new MySQLConnection();
		try {
			con.executeUpdate(cis.toString());
			LOG.debug("Executed " + cis);
		} catch (SQLException e) {
			try {
				java.sql.ResultSet rs = con.executeQuery("SHOW INDEX FROM " + cis.getTableName() + " WHERE key_name='" + cis.getIndexName() + "'");
				int n = 0;
				boolean good = true;
				while (rs.next()) {
					String colName = rs.getString("Column_name");
					int pos = rs.getInt("Seq_in_index");
					n++;
					if (!cis.getColumnNames().get(pos - 1).equalsIgnoreCase(colName)) {
						good = false;
						break;
					}
				}
				if (good && cis.getColumnNames().size() == n) {
					LOG.debug("Exisiting index " + cis + " is good");
					return;
				}
				/*
				 * Index does not match existing one drop and try again - but make sure it works with maximum size of
				 * index - total bytes and column count
				 */
				if (n != 0) {
					try {
						con.executeUpdate("DROP INDEX " + cis.getIndexName() + " ON " + cis.getTableName());
						LOG.debug("Executed " + "DROP INDEX " + cis.getIndexName() + " ON " + cis.getTableName());
					} catch (Exception e1) {
						throw new RGMAPermanentException(e1);
					}
				}
				StringBuffer buf = new StringBuffer("CREATE INDEX ");
				buf.append(cis.getIndexName()).append(" ON ").append(cis.getTableName()).append(" (");
				int colNum = 0;
				int ncolIndex = Math.min(MaxColInIndex, cis.getColumnNames().size());
				int maxStringColumnInIndex = MaxBytesInIndex / ncolIndex;
				for (String colName : cis.getColumnNames()) {
					String type;
					try {
						rs = con.executeQuery("DESCRIBE " + cis.getTableName() + " " + colName);
						rs.first();
						type = rs.getString("Type");
					} catch (Exception e1) {
						throw new RGMAPermanentException(e1);
					}
					buf.append(colName);
					if (type.startsWith("varchar")) {
						String lenString = type.substring(8, type.length() - 1);
						int lenInt = Integer.parseInt(lenString);
						if (maxStringColumnInIndex < lenInt) {
							buf.append("(" + maxStringColumnInIndex + ")");
						}
					}
					if (colNum < ncolIndex - 1) {
						buf.append(", ");
					} else if (colNum == ncolIndex - 1) {
						break;
					}
					colNum++;
				}
				buf.append(')');
				try {
					con.executeUpdate(buf.toString());
					LOG.debug("Executed " + buf);
				} catch (SQLException e1) {
					LOG.error("SQL Exception " + e1.getMessage() + " while creating index " + buf);
				}
			} catch (SQLException e1) {
				LOG.error("SQL Exception " + e1.getMessage() + " while creating index " + cis);
			}
		} finally {
			con.close();
		}
	}

	/**
	 * Gets the existing table name for the passed on parameters. If no table is found then it creates an entry.
	 */
	public synchronized String createTable(String ownerDN, String logicalName, String vdbTableName, String tableType, CreateTableStatement cts)
			throws RGMAPermanentException {
		java.sql.ResultSet rs = null;
		MySQLConnection con = null;
		String physicalTableName = null;
		try {
			con = new MySQLConnection();
			rs = con.executeQuery("select physicalTableName from TupleStore_Mapping where ownerDN='" + ownerDN + "' AND logicalName='" + logicalName
					+ "' AND vdbTableName='" + vdbTableName + "' AND tableType='" + tableType + "'");
			boolean tableExists;
			if (rs.next()) {
				physicalTableName = rs.getString("physicalTableName");
				tableExists = con.executeQuery("SHOW TABLES LIKE '" + physicalTableName + "'").next();
			} else {
				while (true) { /* Look for a free table name */
					physicalTableName = "PT" + m_random.nextInt(Integer.MAX_VALUE);
					rs = con.executeQuery("SHOW TABLES LIKE '" + physicalTableName + "'");
					if (!rs.next()) { /* it's free */
						if (logicalName.length() > 0) {
							con.executeUpdate("REPLACE INTO TupleStore_Mapping VALUES ('" + ownerDN + "','" + logicalName + "','" + vdbTableName + "','"
									+ tableType + "','" + physicalTableName + "')");
						}
						if (LOG.isDebugEnabled()) {
							LOG.debug("New TupleStore_Mapping entry added");
						}
						break;
					}
				}
				tableExists = false;

			}
			CreateTableStatement pcts = new CreateTableStatement(physicalTableName);
			pcts.setColumns(new ArrayList<ColumnDefinition>(cts.getColumns()));
			if (tableType.equals("H")) {
				pcts.getColumns().add(ReservedColumns.RGMA_INSERT_TIME_COLUMN);
				pcts.getColumns().add(ReservedColumns.RGMA_TUID_COLUMN);
			}

			if (tableExists) {/* Check the table is good */
				CreateTableStatement existingTable = describeTable(con, physicalTableName);
				if (!existingTable.equals(pcts)) {
					fixTable(con, existingTable, pcts, tableType);
				}
			} else {
				con.executeUpdate(translateCreateTable(pcts));
				addIndices(con, physicalTableName, tableType);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Physical table '" + physicalTableName + "' created for " + tableType + " '" + vdbTableName + "' " + logicalName + "/[" + ownerDN
							+ ']');
				}
			}
			return physicalTableName;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	private void addIndices(MySQLConnection con, String physicalTableName, String tableType) throws RGMAPermanentException, SQLException {
		if (tableType.equals("H")) {
			// create an index on RgmaInsertTime to aid cleanup
			String indexUpdate = "CREATE INDEX " + physicalTableName + "HDeleteIndex  ON " + physicalTableName + "("
					+ ReservedColumns.RGMA_INSERT_TIME_COLUMN_NAME + ")";
			con.executeUpdate(indexUpdate);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Index '" + physicalTableName + "HDeleteIndex' created for " + physicalTableName + "(" + ReservedColumns.RGMA_INSERT_TIME_COLUMN_NAME
						+ ")");
			}
		} else {
			// create an index on RgmaInsertTime to aid cleanup
			String indexUpdate = "CREATE INDEX " + physicalTableName + "LDeleteIndex  ON " + physicalTableName + "(" + ReservedColumns.RGMA_LRT_COLUMN_NAME
					+ ")";
			con.executeUpdate(indexUpdate);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Index '" + physicalTableName + "LDeleteIndex' created for " + physicalTableName + "(" + ReservedColumns.RGMA_LRT_COLUMN_NAME + ")");
			}
		}
	}

	private void fixTable(MySQLConnection con, CreateTableStatement existingTable, CreateTableStatement needed, String tableType)
			throws RGMAPermanentException, SQLException {
		Map<String, ColumnDefinition> existingColumns = new HashMap<String, ColumnDefinition>();
		for (ColumnDefinition cd : existingTable.getColumns()) {
			existingColumns.put(cd.getName().toUpperCase(), cd);
		}
		Map<String, ColumnDefinition> neededColumns = new HashMap<String, ColumnDefinition>();
		for (ColumnDefinition cd : needed.getColumns()) {
			neededColumns.put(cd.getName().toUpperCase(), cd);
		}

		/* New columns to add */
		Set<String> toAdd = new HashSet<String>(neededColumns.keySet());
		toAdd.removeAll(existingColumns.keySet());

		/* Old columns to remove */
		Set<String> toRemove = new HashSet<String>(existingColumns.keySet());
		toRemove.removeAll(neededColumns.keySet());

		/* Columns in both which may need modifying - to be updated below */
		Set<String> toModify = new HashSet<String>(neededColumns.keySet());
		toModify.retainAll(existingColumns.keySet());

		/* Columns to copy from the old to the new table */
		Set<String> toCopy = new HashSet<String>(toModify);

		boolean modifiable = true;
		for (String c : toAdd) {
			if (neededColumns.get(c).isNotNull()) {
				LOG.info("Adding not null column " + c);
				modifiable = false;
				break;
			}
		}

		if (modifiable) {
			for (String c : toRemove) {
				if (existingColumns.get(c).isPrimaryKey()) {
					LOG.info("Removing primary key column " + c);
					modifiable = false;
					break;
				}
			}
		}

		if (modifiable) {
			Iterator<String> tom = toModify.iterator();
			while (tom.hasNext()) {
				String c = tom.next();
				DataType existingType = existingColumns.get(c).getType();
				DataType neededType = neededColumns.get(c).getType();
				if (existingType.equals(neededType)) {
					tom.remove(); /* They were the same */
				} else if (existingType.getType() == Type.DOUBLE_PRECISION && neededType.getType() == Type.REAL) {
					tom.remove(); /* They were the close enough */
				} else {
					if (existingType.getType() == Type.CHAR || existingType.getType() == Type.VARCHAR) {
						if (neededType.getType() == Type.CHAR || neededType.getType() == Type.VARCHAR) {
							modifiable = neededType.getSize() >= existingType.getSize();
						} else {
							modifiable = false;
						}
					} else {
						modifiable = false;
					}
					if (!modifiable) {
						LOG.info(existingType + " cannot be converted to " + neededType);
						break;
					}
				}
			}
		}

		if (toModify.size() + toAdd.size() + toRemove.size() == 0) {
			return; /* In fact there is nothing to do */
		}

		String tableName = needed.getTableName();
		if (modifiable) {
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (String c : toCopy) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}
				sb.append(c);
			}
			needed.setTableName("TEMP");
			con.executeUpdate(translateCreateTable(needed));
			needed.setTableName(tableName); /* Not really necessary - however the cts is passed into the method */
			con.executeUpdate("INSERT INTO TEMP (" + sb + ") SELECT " + sb + " FROM " + tableName);
			con.executeUpdate("DROP TABLE " + tableName);
			con.executeUpdate("RENAME TABLE TEMP TO " + tableName);
			LOG.info("Table structure changed succesfully");
		} else {
			con.executeUpdate("DROP TABLE " + existingTable.getTableName());
			con.executeUpdate(translateCreateTable(needed));
			LOG.info("Changes cannot be made - table dropped and recreated");
		}

		addIndices(con, tableName, tableType);
	}

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
	public int deleteByHRP(String tableName, int maxAgeSecs, int lastReadTUID) throws RGMAPermanentException {
		if (lastReadTUID == 0) {
			throw new RGMAPermanentException("deleteByHRP does not accept 0 for lastReadTUID");
		}
		try {
			String timestampString = new Timestamp(System.currentTimeMillis() - maxAgeSecs * 1000L).toString();
			String deleteStr;
			if (lastReadTUID > 0) {
				deleteStr = "DELETE FROM " + tableName + " WHERE " + ReservedColumns.RGMA_INSERT_TIME_COLUMN_NAME + " < '" + timestampString + "' AND "
						+ ReservedColumns.RGMA_TUID_COLUMN_NAME + " <= " + lastReadTUID;
			} else {
				deleteStr = "DELETE FROM " + tableName + " WHERE " + ReservedColumns.RGMA_INSERT_TIME_COLUMN_NAME + " < '" + timestampString + "'";
			}
			return MySQLConnection.executeSimpleUpdate(deleteStr);
		} catch (SQLException e) {
			if (e.getErrorCode() == 1146) {
				LOG.debug("Unable to delete from table " + tableName + " as it no longer exists");
				return 0;
			}
			throw new RGMAPermanentException(e);
		}
	}

	/**
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 * @see MySQLTupleStoreDatabase#deleteByLRP(String)
	 */
	public int deleteByLRP(String tableName) throws RGMAPermanentException {
		try {
			return MySQLConnection.executeSimpleUpdate("DELETE FROM " + tableName + " WHERE " + ReservedColumns.RGMA_LRT_COLUMN_NAME + " < NOW()");
		} catch (SQLException e) {
			if (e.getErrorCode() == 1146) {
				LOG.debug("Unable to delete from table " + tableName + " as it no longer exists");
				return 0;
			}
			throw new RGMAPermanentException(e);
		}
	}

	/**
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 * @see org.glite.rgma.server.services.producer.store.TupleStoreList#remove(java.lang.String)
	 */
	public void dropTupleStore(String ownerDN, String logicalName) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			java.sql.ResultSet jrs = con.executeQuery("select physicalTableName from TupleStore_Mapping where ownerDN='" + ownerDN + "' AND logicalName='"
					+ logicalName + "'");
			List<String> physicalTableNames = new ArrayList<String>();
			while (jrs.next()) {
				physicalTableNames.add(jrs.getString(1));
			}
			for (String ptn : physicalTableNames) {
				con.executeUpdate("DROP TABLE " + ptn);
				con.executeUpdate("DELETE FROM TupleStore_TUID WHERE physicalTableName = '" + ptn + "'");
			}
			con.executeUpdate("DELETE FROM TupleStore_Mapping WHERE logicalName = '" + logicalName + "' AND ownerDN = '" + ownerDN + "'");

		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	/**
	 * Retrieves the specified cursor, if it exists, and executes fetch on it.
	 * 
	 * @throws RGMAPermanentException
	 * @see org.glite.rgma.server.services.database.TupleStoreDatabase#fetch(int, int)
	 */
	public TupleSetWithLastTUID fetch(int cursorID, int maxRows) throws RGMAPermanentException {
		try {
			MySQLOneTimeCursor cursor = m_cursors.get(cursorID);
			if (cursor == null) {
				throw new RGMAPermanentException("Cursor " + cursorID + " does not exist.");
			}
			return cursor.fetch(maxRows);
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		}
	}

	/**
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 * @see TupleStoreDatabase#findFirstTupleID(String, long)
	 */
	public int findFirstTupleID(String physicalTableName, long startTimeMS) throws RGMAPermanentException {
		String timestampString = new Timestamp(startTimeMS).toString();
		String findFirstQuery = "SELECT " + ReservedColumns.RGMA_TUID_COLUMN_NAME + " FROM " + physicalTableName + " WHERE "
				+ ReservedColumns.RGMA_TIMESTAMP_COLUMN_NAME + " >= '" + timestampString + "' ORDER BY " + ReservedColumns.RGMA_TUID_COLUMN_NAME + " LIMIT 1";
		int firstTupleID = 0; /* First tuple ID is 0 if there are no tuples */
		MySQLConnection con = null;
		java.sql.ResultSet findFirstResult = null;
		try {
			con = new MySQLConnection();
			findFirstResult = con.executeQuery(findFirstQuery);
			if (findFirstResult.next()) {
				firstTupleID = findFirstResult.getInt(1);
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return firstTupleID;
	}

	public Map<ResourceEndpoint, Integer> getConsumerTUIDs(String physicalTableName) throws RGMAPermanentException {
		MySQLConnection con = null;
		Map<ResourceEndpoint, Integer> result = new HashMap<ResourceEndpoint, Integer>();
		try {
			con = new MySQLConnection();
			java.sql.ResultSet jrs = con.executeQuery("SELECT consumerURL, consumerID, TUID FROM TupleStore_TUID WHERE physicalTableName = '"
					+ physicalTableName + "'");
			while (jrs.next()) {
				ResourceEndpoint ep = new ResourceEndpoint(new URL(jrs.getString(1)), jrs.getInt(2));
				result.put(ep, jrs.getInt(3));
			}
			return result;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public TupleSetWithLastTUID getContinuous(SelectStatement select, int maxCount) throws RGMAPermanentException {
		MySQLConnection con = new MySQLConnection();
		String selectString = select + " LIMIT " + maxCount;
		try {
			java.sql.ResultSet resultSet = con.executeQuery(selectString);
			TupleSetWithLastTUID rs = convertResultSet(resultSet);
			return rs;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public int getMaxTUID(String physicalTableName) throws RGMAPermanentException {
		MySQLConnection con = null;
		java.sql.ResultSet jrs = null;
		try {
			con = new MySQLConnection();
			jrs = con.executeQuery("SELECT MAX(RgmaTUID) FROM " + physicalTableName);
			jrs.next();
			return jrs.getInt(1);
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	/**
	 * @throws RGMAPermanentException
	 * @see org.glite.rgma.server.services.database.TupleStoreDatabase#insert(java.util.List, java.lang.String,
	 *      java.util.List)
	 */
	public void insert(InsertStatement insertStatement) throws RGMAPermanentException {
		try {
			MySQLConnection.executeSimpleUpdate(insertStatement.toString());
		} catch (SQLException e) {
			LOG.error(insertStatement.toString());
			throw new RGMAPermanentException(e);
		}
	}

	/**
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 * @see org.glite.rgma.server.services.producer.store.TupleStoreList#listTupleStores(String)
	 */
	public List<TupleStoreDetails> listTupleStores(String userDN) throws RGMAPermanentException {
		MySQLConnection con = null;
		if (userDN == null) {
			userDN = "";
		}
		List<TupleStoreDetails> tsds = new ArrayList<TupleStoreDetails>();
		try {
			Map<String, Boolean> isLatest = new HashMap<String, Boolean>();
			con = new MySQLConnection();
			java.sql.ResultSet result = con.executeQuery("SELECT DISTINCT logicalName, tableType from TupleStore_Mapping WHERE ownerDN = '" + userDN
					+ "' ORDER BY logicalName");
			while (result.next()) {
				String logicalName = result.getString("logicalName");
				String tableType = result.getString("tableType");
				if (tableType.equals("L")) {
					isLatest.put(logicalName, true);
				} else {
					if (!isLatest.containsKey(logicalName)) {
						isLatest.put(logicalName, false);
					}
				}
			}
			for (Entry<String, Boolean> entry : isLatest.entrySet()) {
				TupleStoreDetails tsd = new TupleStoreDetails(StorageType.DB, entry.getKey(), userDN, entry.getValue());
				tsds.add(tsd);
			}
			return tsds;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	/**
	 * Creates a Cursor for the given SELECT statement and adds it to the list of cursors. Each cursor has its own
	 * connection to MySQL.
	 * 
	 * @throws RGMAPermanentException
	 * @see org.glite.rgma.server.services.database.TupleStoreDatabase#openCursor(org.glite.rgma.server.services.sql.SelectStatement)
	 */
	public int openCursor(SelectStatement selectStatement) throws RGMAPermanentException {
		int cursorID = nextCursorID();
		MySQLOneTimeCursor cursor = new MySQLOneTimeCursor(cursorID, selectStatement);
		m_cursors.put(cursorID, cursor);
		return cursorID;
	}

	/**
	 * Executes the SELECT statement on the database and converts the resulting data into an R-GMA ResultSet (including
	 * meta-data).
	 * 
	 * @throws RGMAPermanentException
	 * @see org.glite.rgma.server.services.database.TupleStoreDatabase#select(org.glite.rgma.server.services.sql.SelectStatement)
	 */
	public TupleSetWithLastTUID select(SelectStatement selectStatement) throws RGMAPermanentException {
		String select = selectStatement.toString();
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			java.sql.ResultSet jdbcResultSet = con.executeQuery(select);
			TupleSetWithLastTUID resultSet = convertResultSet(jdbcResultSet);
			return resultSet;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public void shutdown() {
	/* Nothing to do */
	}

	public void storeConsumerTUIDs(String physicalTableName, Map<ResourceEndpoint, Integer> consumerTUIDs) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			con.executeUpdate("DELETE FROM TupleStore_TUID WHERE physicalTableName = '" + physicalTableName + "'");
			if (consumerTUIDs.size() > 0) {
				StringBuilder s = new StringBuilder("INSERT INTO TupleStore_TUID (physicalTableName , consumerURL, consumerID, TUID) VALUES");
				boolean first = true;
				for (Entry<ResourceEndpoint, Integer> ct : consumerTUIDs.entrySet()) {
					ResourceEndpoint re = ct.getKey();
					if (first) {
						first = false;
					} else {
						s.append(',');
					}
					s.append("('" + physicalTableName + "','" + re.getURL() + "'," + re.getResourceID() + "," + ct.getValue() + ")");
				}
				con.executeUpdate(s.toString());
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	/**
	 * Executes the given update statement on the database.
	 * 
	 * @throws RGMAPermanentException
	 * @see org.glite.rgma.server.services.database.TupleStoreDatabase#update(org.glite.rgma.server.services.sql.UpdateStatement)
	 */
	public int update(UpdateStatement updateStatement) throws RGMAPermanentException {
		try {
			return MySQLConnection.executeSimpleUpdate(updateStatement.toString());
		} catch (SQLException e) {
			LOG.error(updateStatement.toString());
			throw new RGMAPermanentException(e);
		}
	}

	/**
	 * Gets a description of the given table using "DESCRIBE tableName".
	 * 
	 * @throws RGMAPermanentException
	 * @see org.glite.rgma.server.services.database.TupleStoreDatabase#describeTable(java.lang.String)
	 */
	private CreateTableStatement describeTable(MySQLConnection con, String tableName) throws SQLException, RGMAPermanentException {
		java.sql.ResultSet resultSet = con.executeQuery("DESCRIBE " + tableName);
		/* Returns { Field, Type, Null, Key, Default, Extra } */
		CreateTableStatement createTable = new CreateTableStatement(tableName);
		List<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
		createTable.setColumns(columns);
		while (resultSet.next()) {
			ColumnDefinition colDef = new ColumnDefinition();
			colDef.setName(resultSet.getString(1));
			colDef.setType(translateTypeString(resultSet.getString(2)));
			colDef.setNotNull(!resultSet.getString(3).equals("YES"));
			colDef.setPrimaryKey(resultSet.getString(4).equals("PRI"));
			columns.add(colDef);
		}
		return createTable;
	}
}