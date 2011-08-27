/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * Connection to HSQLDB RDBMS.
 */
public class HSQLDBConnection {
	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger(ServerConstants.SECURITY_LOGGER);

	/** Driver name. */
	private static final String JDBC_DRIVER = "org.hsqldb.jdbcDriver";

	private static int s_logWidth;

	/** JDBC connection to HSQLDB DB. */
	private Connection m_connection;

	/** Database URL. */
	private String m_url;

	/** Database user name. */
	private String m_user;

	/** Database password. */
	private String m_password;

	/**
	 * Creates a new HSQLDB connection to the specified database. Does not yet establish connection, just instantiates
	 * driver.
	 * 
	 * @param url
	 *            DB URL.
	 * @param user
	 *            User name.
	 * @param password
	 *            Password.
	 * @throws DatabaseException
	 *             If the JDBC driver can't be found.
	 */
	public HSQLDBConnection(String url, String user, String password) throws RGMAPermanentException {
		try {
			ServerConfig config = ServerConfig.getInstance();
			s_logWidth = config.getInt(ServerConstants.DATABASE_LOG_WIDTH);
			Class.forName(JDBC_DRIVER);
			m_url = url;
			m_user = user;
			m_password = password;
		} catch (ClassNotFoundException e) {
			throw new RGMAPermanentException(e);
		}
	}

	/**
	 * Executes an update on this HSQLDB database.
	 * 
	 * @param update
	 *            SQL statement.
	 * @return Number of rows updated or <code>0</code> if statement has no return value.
	 * @throws DatabaseException
	 *             If the execute failed.
	 * @throws SQLException
	 */
	public int executeUpdate(String update) throws SQLException {
		Statement statement = null;

		try {
			if (LOG.isDebugEnabled()) {
				int n = Math.min(s_logWidth, update.length());
				LOG.debug("Executing update: " + update.substring(0, n));
			}

			if (m_connection == null) {
				m_connection = DriverManager.getConnection(m_url, m_user, m_password);
			}

			statement = m_connection.createStatement();

			int updateCount = statement.executeUpdate(update);

			return updateCount;
		} finally {
			closeStatement(statement);
		}
	}

	/**
	 * Closes this HSQLDB connection.
	 */
	public void close() {
		if (m_connection != null) {
			try {
				m_connection.close();
			} catch (SQLException e) {
				// Do nothing
			}
		}
		m_connection = null;
	}

	/**
	 * Executes an update on this HSQLDB database. It is the responsibility of the calling program to close the
	 * ResultSet/Statement if this method succeeds.
	 * 
	 * @param query
	 *            SQL query.
	 * @throws DatabaseException
	 *             If the execute failed.
	 * @throws SQLException
	 */
	public ResultSet executeQuery(String query) throws SQLException {
		Statement statement = null;

		if (LOG.isDebugEnabled()) {
			int n = Math.min(s_logWidth, query.length());
			LOG.debug("Executing query: " + query.substring(0, n));
		}

		if (m_connection == null) {
			m_connection = DriverManager.getConnection(m_url, m_user, m_password);
		}

		statement = m_connection.createStatement();

		return statement.executeQuery(query);
	}

	/**
	 * Closes the given statement if it exists.
	 * 
	 * @param statement
	 *            Statement to close.
	 */
	static void closeStatement(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				LOG.error("Error closing statement.", e);
			}
		}
	}

	/**
	 * Closes the statement that created the given ResultSet.
	 * 
	 * @param result
	 *            A ResultSet object.
	 */
	public static void closeStatementFromResultSet(ResultSet result) {
		if (result != null) {
			try {
				closeStatement(result.getStatement());
			} catch (SQLException e) {
				LOG.debug("Error closing statement from ResultSet.", e);
			}
		}
	}
}
