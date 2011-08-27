/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.registry;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * DOCUMENT ME!
 * 
 * @author $author$
 * @version $Revision: 1.11 $
 */
public class RegistryDatabaseFactory {
	private static Logger m_log = Logger.getLogger(RegistryConstants.REGISTRY_DATABASE_LOGGER);

	/**
	 * Do not instantiate use the static methods
	 */
	protected RegistryDatabaseFactory() {

	}

	static RegistryDatabase getRegistryDatabase(String vdbName) throws RGMAPermanentException {
		RegistryDatabase db = null;
		ServerConfig config = ServerConfig.getInstance();
		String dbType = config.getString(ServerConstants.DATABASE_TYPE);
		if (dbType.equals(ServerConstants.MYSQL_DB_TYPE)) {
			db = new MySQLRegistryDatabase(vdbName);
		} else if (dbType.equals(ServerConstants.ORACLE_DB_TYPE)) {
			m_log.error("Oracle Database not implemented yet");
			throw new RGMAPermanentException("Oracle Database not implemented yet");
		} else {
			m_log.error("Could not instantiate database, unknown type: " + dbType);
			throw new RGMAPermanentException("Could not instantiate database, unknown type: " + dbType);
		}

		return db;
	}
}
