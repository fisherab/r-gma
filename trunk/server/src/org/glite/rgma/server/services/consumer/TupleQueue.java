/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.consumer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.database.MySQLConnection;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.TupleSet;

/**
 * A queue of tuples stored on a consumer before they are popped by the user. The buffer is implemented using a queue in
 * memory and a table in the database. Data are added to the queue, but if it gets too full the whole contents of the
 * queue are saved in the database table with an extra SEQ column to preserve the ordering of the tuples. When
 * retrieving the data memory is only used when the database table is empty. Read and write SEQ values are saved to
 * avoid trivial calls to the database. Synchronization is necessary as many replies may be writing to the TupleQueue
 * and once as well as a single user popping data.
 */
public class TupleQueue {
	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger("rgma.services.consumer");

	/** "Read pointer" - lowest SEQ value in the database */
	private int m_read = 0;

	/** "Write pointer" - next SEQ value to write to the database */
	private int m_write = 0;

	/** To show that database table has been created */
	private boolean m_tableCreated = false;

	/** Maximum count of tuples to hold in memory */
	private int m_maxTuplesMem;

	/** Name of data base table */
	private String m_databaseTableName;

	/** Number of columns in the table - set when the table is created */
	private int m_columnCount;

	/** Maximum number of tuples to hold in the database */
	private int m_maxTuplesDB;

	/** A queue implemented as a linked list */
	private Queue<String[]> m_queue;

	/** Number of tuples to write together to the database */
	private int m_tupleWriteBatchSize;

	private String m_warning;

	/**
	 * Constructor.
	 * 
	 * @param maxTuplesMem
	 *            Maximum number of tuples to be stored in memory
	 * @param maxTuplesDB
	 *            Maximum number of tuples to be stored on disk
	 * @param databaseTableName
	 *            Name for DB table cache file.
	 * @throws RGMAPermanentException
	 * @throws IOException
	 * @throws DatabaseException
	 */
	public TupleQueue(int maxTuplesMem, int maxTuplesDB, int tupleWriteBatchSize, String databaseTableName) throws RGMAPermanentException {
		m_maxTuplesMem = maxTuplesMem;
		m_maxTuplesDB = maxTuplesDB;
		m_tupleWriteBatchSize = tupleWriteBatchSize;
		m_databaseTableName = databaseTableName;
		m_queue = new LinkedList<String[]>();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Created tuple queue of size " + m_maxTuplesMem + " tuples in memory and " + m_maxTuplesDB + " on disk. Tuples stored in batches of "
					+ tupleWriteBatchSize);
		}
	}

	/**
	 * Pushes the tuples onto this TupleStack.
	 * 
	 * @param rs
	 *            ResultSet containing tuples to add.
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 */
	public void push(org.glite.rgma.server.system.TupleSet rs) throws RGMAPermanentException, RGMAPermanentException {
		synchronized (this) {
			String warning = rs.getWarning();
			if (warning != null) {
				m_warning = warning;
			}
			for (String[] tuple : rs.getData()) {
				if (m_queue.size() >= m_maxTuplesMem) {
					if (m_write - m_read > m_maxTuplesDB) { // Check on how many tuples on disk
						throw new RGMAPermanentException("Tuple Queue is full - you must pop faster");
					}

					try {
						if (!m_tableCreated) { // Make sure the table exists
							m_columnCount = tuple.length;
							StringBuilder create = new StringBuilder("CREATE TABLE " + m_databaseTableName + "(SEQ INT PRIMARY KEY");
							for (int i = 0; i < m_columnCount; i++) {
								create.append(" ,COL" + i + " TEXT");
							}
							create.append(" , INDEX (SEQ)) ENGINE MYISAM");
							MySQLConnection.executeSimpleUpdate(create.toString());
							m_tableCreated = true;
						}

						StringBuffer insert = new StringBuffer().append("INSERT INTO ").append(m_databaseTableName).append(" VALUES");
						boolean empty = true;
						for (int i = 0; i < m_maxTuplesMem; i++) {
							String[] tupleToMove = m_queue.remove();

							if (empty) {
								empty = false;
							} else {
								insert.append(", ");
							}
							insert.append("(").append(m_write++);
							for (String value : tupleToMove) {
								if (value == null) {
									insert.append(" ,null");
								} else {
									insert.append(" ,'").append(value.replace("'", "''")).append("'");
								}
							}
							insert.append(')');

							if ((i + 1) % m_tupleWriteBatchSize == 0) {
								MySQLConnection.executeSimpleUpdate(insert.toString());
								insert = new StringBuffer().append("INSERT INTO ").append(m_databaseTableName).append(" VALUES");
								empty = true;
							}
						}
						if (!empty) { // Store any tuples not yet written
							MySQLConnection.executeSimpleUpdate(insert.toString());
						}
					} catch (SQLException e) {
						throw new RGMAPermanentException(e);
					}
				}
				m_queue.add(tuple); // and add the tuple to the queue
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug(rs.getData().size() + " tuples just stored in TupleQueue. New status is: MSIZE=" + m_queue.size() + " Readptr=" + m_read
						+ " Writeptr=" + m_write + " Count=" + (m_queue.size() + m_write - m_read));
			}
		}

	}

	/**
	 * Attempts to pop at most <code>maxNumTuples</code> tuples from the queue and disk cache.
	 * 
	 * @param maxNumTuples
	 *            The maximun number of tuples to return.
	 * @return ResultSet The popped tuples; empty if no tuples exist.
	 * @throws RGMAPermanentException
	 */
	public org.glite.rgma.server.system.TupleSet pop(int maxNumTuples) throws RGMAPermanentException {

		List<String[]> tuples = new ArrayList<String[]>();
		synchronized (this) {

			int last = Math.min(m_read + maxNumTuples, m_write) - 1;

			if (m_read != last + 1) {

				String tableAndRange = new StringBuffer().append("FROM ").append(m_databaseTableName).append(" WHERE (SEQ BETWEEN ").append(m_read).append(
						" AND ").append(last).append(')').toString();

				MySQLConnection conn = null;
				try {
					conn = new MySQLConnection();

					java.sql.ResultSet jrs = conn.executeQuery("SELECT * " + tableAndRange);
					conn.executeUpdate("DELETE " + tableAndRange);

					m_read = last + 1;

					while (jrs.next()) {
						String[] tuple = new String[m_columnCount];
						for (int i = 0; i < m_columnCount; i++) {
							tuple[i] = jrs.getString(i + 2);
						}
						tuples.add(tuple);
					}
				} catch (SQLException e) {
					throw new RGMAPermanentException(e);
				} finally {
					if (conn != null) {
						conn.close();
					}
				}

			}
			/* Number of tuples to get from the queue */
			int need = Math.min(maxNumTuples - tuples.size(), m_queue.size());
			for (int i = 0; i < need; i++) {
				tuples.add(m_queue.remove());
			}

			TupleSet answer; // Build the result set
			answer = new TupleSet();
			if (tuples.size() != 0) {
				answer.addRows(tuples);
				if (LOG.isDebugEnabled()) {
					LOG.debug(tuples.size() + " tuples just retrieved from TupleQueue. New status is: MSIZE=" + m_queue.size() + " Readptr=" + m_read
							+ " Writeptr=" + m_write + " Count=" + (m_queue.size() + m_write - m_read));
				}
			}
			if (m_warning != null) {
				answer.setWarning(m_warning);
			}
			return answer;
		}
	}

	public boolean isEmpty() {
		synchronized (this) {
			return m_queue.isEmpty(); // There cannot be data on disk if none in memory
		}
	}

	public int numTuplesMem() {
		synchronized (this) {
			return m_queue.size();
		}
	}

	public int numTuplesDB() {
		synchronized (this) {
			return m_write - m_read;
		}
	}

	public void close() throws RGMAPermanentException {
		synchronized (this) {
			try {
				if (m_tableCreated) {
					StringBuffer drop = new StringBuffer().append("DROP TABLE ").append(m_databaseTableName);
					MySQLConnection.executeSimpleUpdate(drop.toString());
				}
			} catch (SQLException e) {
				throw new RGMAPermanentException(e);
			}
		}
	}

	public static void dropOldTables(String prefix) throws RGMAPermanentException {
		MySQLConnection conn = null;
		try {
			conn = new MySQLConnection();
			StringBuffer list = new StringBuffer().append("SHOW TABLES LIKE \"").append(prefix).append("%\"");
			java.sql.ResultSet jrs = conn.executeQuery(list.toString());
			List<String> tables = new ArrayList<String>();
			while (jrs.next()) {
				tables.add(jrs.getString(1));
			}
			for (String table : tables) {
				StringBuffer drop = new StringBuffer().append("DROP TABLE ").append(table);
				conn.executeUpdate(drop.toString());
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
}
