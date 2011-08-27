/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.registry;

import org.apache.log4j.Logger;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.Units;

/**
 * Thread that will cleanup the registry database every service defined cleanup
 * period
 * 
 */
class RegistryCleanupThread extends Thread {
	private static final Logger LOG = Logger.getLogger(RegistryConstants.REGISTRY_CLEANUP_LOGGER);

	/** The registry database */
	private RegistryDatabase m_registryDatabase;

	/** The time interval before cleaning up the database */
	private long m_cleanupIntervalMillis;

	private boolean m_shutdown = false;

	private boolean m_initialised = false;

	/**
	 * This is the thread that will periodically cleanup the database
	 * 
	 * @param registryDatabase
	 */
	RegistryCleanupThread(RegistryDatabase registryDatabase, TimeInterval cleanupInterval) {
		m_registryDatabase = registryDatabase;
		m_cleanupIntervalMillis = cleanupInterval.getValueAs(Units.MILLIS);
	}

	public void run() {
		while (!m_shutdown) {
			try {
				m_registryDatabase.purgeExpiredRegistrations();
				m_initialised = true;
			} catch (RGMAPermanentException rgmae) {
				LOG.error("Error occured purging expired registrations " + rgmae.getMessage());
			}
			try {
				Thread.sleep(m_cleanupIntervalMillis);
			} catch (InterruptedException e) {
				/* Nothing to do */
			}
		}
	}

	/**
	 * Has this thread been through at least one cleanup cycle
	 */
	boolean isInitialised() {
		return m_initialised;
	}

	void shutdown() {
		LOG.info("Shutdown received by registry vdb, setting shutdown to true");
		m_shutdown = true;
		interrupt();
	}
}
