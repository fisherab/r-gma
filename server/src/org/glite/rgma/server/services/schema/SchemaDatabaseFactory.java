package org.glite.rgma.server.services.schema;

import java.net.URL;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.system.RGMAPermanentException;

public class SchemaDatabaseFactory {
	private static Logger m_log = Logger.getLogger(SchemaConstants.SCHEMA_LOGGER);

	private SchemaDatabaseFactory() {
	}

	public static SchemaDatabase getSchemaDatabase(String vdbName, URL masterURL, URL serviceURL, List<String> rules) throws  RGMAPermanentException {
		SchemaDatabase db = null;
		ServerConfig config = ServerConfig.getInstance();
		String dbType = config.getProperty(ServerConstants.DATABASE_TYPE);
		if (dbType == null) {
			throw new RGMAPermanentException("Schema database type not found in config file");
		}
		if (dbType.equals(ServerConstants.MYSQL_DB_TYPE)) {
			db = new MySQLSchemaDatabase(vdbName, masterURL, serviceURL, rules);
		} else if (dbType.equals(ServerConstants.ORACLE_DB_TYPE)) {
			m_log.error("Oracle Database not implemented yet");
			throw new RGMAPermanentException("Oracle Database not implemented yet");
		} else if (dbType.equals(ServerConstants.HSQL_DB_TYPE)) {
			m_log.error("HSQL Database not implemented yet");
			throw new RGMAPermanentException("HSQL Database not implemented yet");
		} else {
			m_log.error("Could not instantiate database, unknown type: " + dbType);
			throw new RGMAPermanentException("Could not instantiate database, unknown type: " + dbType);
		}
		return db;
	}
}
