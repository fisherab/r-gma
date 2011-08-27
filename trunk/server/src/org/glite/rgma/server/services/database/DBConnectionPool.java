package org.glite.rgma.server.services.database;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.ConfigurationException;

import org.apache.commons.dbcp.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp.datasources.SharedPoolDataSource;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * Generic database conection pool. Configuration is taken from ServerConfiguration properties obtained from the
 * rgma-server.properties file. This class should not be used directly use the classes such as
 * <code>rg.glite.rgma.server.services.database.mysql.MySQLConnector<code>
 */
class DBConnectionPool {
	private SharedPoolDataSource m_dataSource = null;

	/**
	 * Using this constructor creates a database pool using details that will be obtained from the generic configuration
	 * parameters: database.location.url database.username database.password database.jdbc.driver
	 * database.connection.pool.maxActive database.connection.pool.maxIdle database.connection.pool.maxWait
	 * 
	 * @throws ConfigurationException
	 */
	DBConnectionPool() throws RGMAPermanentException {
		ServerConfig config = ServerConfig.getInstance();
		DriverAdapterCPDS cpds = new DriverAdapterCPDS();
		try {
			cpds.setDriver(config.getString(ServerConstants.DATABASE_JDBC_DRIVER));
		} catch (ClassNotFoundException e) {
			throw new RGMAPermanentException(e);
		}
		cpds.setUrl(config.getString(ServerConstants.DATABASE_LOCATION_URL));
		cpds.setUser(config.getString(ServerConstants.DATABASE_USERNAME));
		cpds.setPassword(config.getString(ServerConstants.DATABASE_PASSWORD));
		m_dataSource = new SharedPoolDataSource();
		m_dataSource.setConnectionPoolDataSource(cpds);
		m_dataSource.setMaxActive(config.getInt(ServerConstants.DATABASE_CONNECTION_POOL_MAX_ACTIVE));
		m_dataSource.setMaxIdle(config.getInt(ServerConstants.DATABASE_CONNECTION_POOL_MAX_IDLE));
		m_dataSource.setMaxWait(config.getInt(ServerConstants.DATABASE_CONNECTION_POOL_MAX_WAIT_SECS) * 1000);
		m_dataSource.setValidationQuery("SELECT 1");
		m_dataSource.setTestOnBorrow(true);
	}

	/**
	 * Returns a connection to this database, creating one if necessary.
	 * 
	 * @return A Connection object to the configured database.
	 * @throws SQLException
	 * @throws RGMAPermanentException
	 */
	Connection getConnection() throws SQLException, RGMAPermanentException {
		if (m_dataSource == null) {
			throw new RGMAPermanentException("Datasource is null database pool not setup");
		}
		return m_dataSource.getConnection();
	}
}
