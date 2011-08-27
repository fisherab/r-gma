/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * Connection to MySQL RDBMS. This is backed by a Connection pool so connections must be closed after they have been
 * finished with. If they are not closed they are not returned to the pool and are not available to be used.
 */
public class MySQLConnection {

	/** As MySQSL operations are potentially sensitive all logging goes to the security logger */
	private static final Logger LOG = Logger.getLogger(ServerConstants.SECURITY_LOGGER);

	/** The connection pool */
	private static DBConnectionPool m_pool = null;

	private static int s_logWidth;

	public static int executeSimpleUpdate(String update) throws RGMAPermanentException, SQLException {
		MySQLConnection conn = null;
		try {
			conn = new MySQLConnection();
			int count = conn.executeUpdate(update);
			return count;
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

	public synchronized static void init() throws RGMAPermanentException {
		if (m_pool == null) {
			ServerConfig config = ServerConfig.getInstance();
			s_logWidth = config.getInt(ServerConstants.DATABASE_LOG_WIDTH);
			m_pool = new DBConnectionPool();
			Statement statement = null;
			Connection con = null;
			try {
				con = m_pool.getConnection();
				statement = con.createStatement();
				statement.executeQuery("SELECT 1");
			} catch (SQLException e) {
				throw new RGMAPermanentException(e);
			} finally { // make sure the connections are released to the pool if an exception occurs
				if (statement != null) {
					try {
						statement.close();
					} catch (SQLException e) {}
				}
				if (con != null) {
					try {
						con.close();
					} catch (SQLException e) {}
				}
			}
		}
	}

	/** JDBC connection to MySQL DB. */
	private Connection m_connection;

	/** Statements that have been executed - this allows the close() to clean up */
	private final List<Statement> m_statements = new ArrayList<Statement>();

	public MySQLConnection() throws RGMAPermanentException {
		if (m_pool == null) {
			throw new RGMAPermanentException("Database connection pool has not been initialised");
		}
	}

	/**
	 * Closes this MySQL connection. The action of closing cleans up but the MySQLConnection can be reused and closed
	 * again.
	 */
	public void close() {
		for (Statement statement : m_statements) { // close all statements first
			try {
				if (statement != null) {
					statement.close();
				}
			} catch (SQLException e) { // Do nothing
			}
		}
		m_statements.clear();
		if (m_connection != null) {
			try {
				m_connection.close();
			} catch (SQLException e) { // Do nothing
			} finally {
				m_connection = null;
			}
		}
	}

	/**
	 * Executes an update on this MySQL database. It is the responsibility of the calling program to close the
	 * ResultSet/Statement if this method succeeds.
	 * 
	 * @param query
	 *            SQL query.
	 * @throws SQLException
	 * @throws RGMAPermanentException
	 * @throws TableNotFoundException
	 *             If the table is not found
	 */
	public ResultSet executeQuery(String query) throws SQLException, RGMAPermanentException {

		Statement statement = null;

		ResultSet rs = null;
		try {
			if (LOG.isDebugEnabled()) {
				int n = Math.min(s_logWidth, query.length());
				LOG.debug("Executing query: " + query.substring(0, n));
			}
			if (m_connection == null) {
				getConnection();
			}
			statement = m_connection.createStatement();
			m_statements.add(statement);
			rs = statement.executeQuery(query);
		} catch (SQLException e) {
			String sqlState = e.getSQLState();
			if (MySQLConstants.MYSQL_COMMUNICATION_ERROR_STATE.equals(sqlState) || MySQLConstants.MYSQL_DEADLOCK_ERROR_STATE.equals(sqlState)) {
				close();
			}
			throw e;
		}
		return rs;
	}

	/**
	 * Executes an update on this MySQL database.
	 * 
	 * @param update
	 *            SQL statement.
	 * @return Number of rows updated or <code>0</code> if statement has no return value.
	 * @throws SQLException
	 * @throws RGMAPermanentException
	 */
	public int executeUpdate(String update) throws RGMAPermanentException, SQLException {
		if (m_connection == null) {
			getConnection();
		}
		int updateCount = 0;
		try {
			if (LOG.isDebugEnabled()) {
				int n = Math.min(s_logWidth, update.length());
				LOG.debug("Executing update: " + update.substring(0, n));
			}
			Statement statement = m_connection.createStatement();
			m_statements.add(statement);
			updateCount = statement.executeUpdate(update);
		} catch (SQLException e) {
			String sqlState = e.getSQLState();
			if (MySQLConstants.MYSQL_COMMUNICATION_ERROR_STATE.equals(sqlState) || MySQLConstants.MYSQL_DEADLOCK_ERROR_STATE.equals(sqlState)) {
				close();
			}
			throw e;
		}
		return updateCount;
	}

	@Override
	protected void finalize() {
		close();
	}

	private void getConnection() throws RGMAPermanentException, SQLException {
		m_connection = m_pool.getConnection();
		Statement statement = null;
		try {
			statement = m_connection.createStatement();
			statement.executeUpdate("set time_zone = '+00:00'");
		} catch (SQLException e) {
			String sqlState = e.getSQLState();
			if (MySQLConstants.MYSQL_COMMUNICATION_ERROR_STATE.equals(sqlState) || MySQLConstants.MYSQL_DEADLOCK_ERROR_STATE.equals(sqlState)) {
				close();
			}
			throw e;
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
			} catch (SQLException e) { // Do nothing
			}
		}
	}
}
