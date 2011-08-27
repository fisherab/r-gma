/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.registry;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.system.ConsumerEntry;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.Units;

class RegistryInstance {

	private static Logger LOG = Logger.getLogger(RegistryConstants.REGISTRY_LOGGER);
	private static Logger LOG_REP = Logger.getLogger(RegistryConstants.REGISTRY_REPLICATION_LOGGER);

	// Used to access the registry database
	private RegistryDatabase m_registryDatabase;

	// the thread used to cleanup the registry database
	private RegistryCleanupThread m_cleanupThread;

	// the namespace of the virtual database that this instance represents
	private String m_vdbName;

	// operational status of this registry instance
	private boolean m_online;

	private Object m_replicationTableEntriesLock = new Object();

	private Map<String, ReplicationTableEntry> m_ReplicationTable = new HashMap<String, ReplicationTableEntry>();

	private ReplicationMessage m_fullUpdateRecords;
	private Timer m_timer;

	/**
	 * Creates a new RegistryInstance object.
	 * 
	 * @param vdbName
	 *            the virtual database name of this registry instance
	 * @param remotesExist
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 */
	RegistryInstance(String vdbName, boolean remotesExist) throws RGMAPermanentException {

		m_vdbName = vdbName;
		ServerConfig serverConfig = ServerConfig.getInstance();
		m_registryDatabase = RegistryDatabaseFactory.getRegistryDatabase(m_vdbName);
		long delayForPossibleIncomingReplicationMillis;
		if (remotesExist) {
			delayForPossibleIncomingReplicationMillis = (serverConfig.getLong(ServerConstants.REGISTRY_REPLICATION_INTERVAL_SECS) + serverConfig
					.getLong(ServerConstants.REGISTRY_REPLICATION_LAG_SECS)) * 1000;
		} else {
			/* Only local copy exists so just set a short delay - the cleanupThread should run in a few seconds */
			delayForPossibleIncomingReplicationMillis = 1000;
		}
		int cleanupTimeout = serverConfig.getInt(ServerConstants.REGISTRY_CLEANUPTHREAD_INTERVAL_SECS);
		/* start a thread that will cleanup the database */
		m_cleanupThread = new RegistryCleanupThread(m_registryDatabase, new TimeInterval(cleanupTimeout, Units.SECONDS));
		m_cleanupThread.start();
		m_timer = new Timer("Wait for " + vdbName + " registry instance to be ready", true);
		m_timer.schedule(new WaitForRegistryInstance(), delayForPossibleIncomingReplicationMillis, delayForPossibleIncomingReplicationMillis);
	}

	class WaitForRegistryInstance extends TimerTask {

		@Override
		public void run() {
			if (m_cleanupThread.isInitialised()) {
				m_timer.cancel();
				m_online = true;
				LOG.info("Registry instance " + m_vdbName + " is now online");
			} else {
				LOG.warn("Waiting for registry instance " + m_vdbName + " to initialise");
			}
		}
	}

	public CharSequence getConsumerTableEntriesForHostName(String hostName) throws RGMAPermanentException {
		return m_registryDatabase.getConsumerTableEntriesForHostName(hostName);
	}

	public String getConsumerTableEntriesForTableName(String tableName) throws RGMAPermanentException {
		return m_registryDatabase.getConsumerTableEntriesForTableName(tableName);
	}

	public int getLastReplicationTime(String host) throws RGMAPermanentException {
		return m_registryDatabase.getLastReplicationTimeSecs(host);
	}

	/**
	 * Get all producers that match the table name and don't contradict the predicate and are not flagged to be deleted.
	 * If the query is continuous the resource will also be registered, otherwise the last two parameters the
	 * consumerEndpoint and termination interval will be unused.
	 * 
	 * @return a list of ProducerTable entries that match the table names and don't contradict the predicate
	 * @throws RGMATemporaryException
	 */

	public List<ProducerTableEntry> getMatchingProducersForTables(List<String> tableNames, String predicate, QueryProperties queryProperties,
			boolean isSecondary, ResourceEndpoint consumerEndpoint, int terminationIntervalSecs) throws RGMAPermanentException, RGMATemporaryException {
		checkOnLine();

		/* if the resource is continuous register it */
		if (consumerEndpoint != null) {
			RegistryConsumerResource consumer = new RegistryConsumerResource(tableNames.get(0), consumerEndpoint, predicate, terminationIntervalSecs,
					queryProperties, isSecondary);
			m_registryDatabase.addRegistration(consumer, true);
			addReplicaEntry(replicaKey(consumer), consumer);
		}

		return m_registryDatabase.getProducersMatchingPredicate(tableNames, predicate, queryProperties, isSecondary);
	}

	public CharSequence getProducerTableEntriesForHostName(String hostName) throws RGMAPermanentException {
		return m_registryDatabase.getProducerTableEntriesForHostName(hostName);
	}

	public String getProducerTableEntriesForTableName(String tableName) throws RGMAPermanentException {
		return m_registryDatabase.getProducerTableEntriesForTableName(tableName);
	}

	/**
	 * convenience method to get vdb consumer count
	 * 
	 * @param masterOnly
	 * @return
	 * @throws DatabaseException
	 * @throws RGMAPermanentException
	 */
	public int getRegisteredConsumerCount(boolean masterOnly) throws RGMAPermanentException {
		return m_registryDatabase.getRegisteredConsumerCount(masterOnly);
	}

	/**
	 * convenience method to get vdb producer count
	 * 
	 * @param masterOnly
	 * @return
	 * @throws DatabaseException
	 * @throws RGMAPermanentException
	 */
	public int getRegisteredProducerCount(boolean masterOnly) throws RGMAPermanentException {
		return m_registryDatabase.getRegisteredProducerCount(masterOnly);
	}

	public List<String> getUniqueHostNames() throws RGMAPermanentException {
		return m_registryDatabase.getUniqueHostNames();
	}

	public List<String> getUniqueTableNames() throws RGMAPermanentException {
		return m_registryDatabase.getUniqueTableNames();
	}

	private void checkOnLine() throws RGMATemporaryException {
		if (!m_online) {
			throw new RGMATemporaryException("VDB " + m_vdbName + " is currently offline.");
		}
	}

	public List<ConsumerEntry> registerProducerTable(ResourceEndpoint endpoint, String tableName, String predicate, ProducerType producerType, int hrpSec,
			int terminationIntervalSec) throws RGMAPermanentException, RGMATemporaryException {
		checkOnLine();
		RegistryProducerResource producer = new RegistryProducerResource(tableName, endpoint, predicate, terminationIntervalSec, producerType, hrpSec);
		m_registryDatabase.addRegistration(producer, true);
		addReplicaEntry(replicaKey(producer), producer);
		return m_registryDatabase.getConsumersMatchingPredicate(producer);
	}

	private void addReplicaEntry(String key, ReplicationTableEntry e) {
		synchronized (m_replicationTableEntriesLock) {
			m_ReplicationTable.put(key, e);
			if (LOG_REP.isDebugEnabled()) {
				LOG_REP.debug("Added entry to replication table: " + e.plusOrMinus() + key + " for " + m_vdbName + ". It now has " + m_ReplicationTable.size()
						+ " entries.");
			}
		}
	}

	private String replicaKey(RegistryConsumerResource resource) {
		return resource.getEndpoint() + "";
	}

	private String replicaKey(RegistryProducerResource resource) {
		return resource.getEndpoint() + resource.getTableName();
	}

	private String replicaKey(UnregisterConsumerResourceRequest resource) {
		return resource.getEndpoint() + "";
	}

	private String replicaKey(UnregisterProducerTableRequest resource) {
		return resource.getEndpoint() + resource.getTableName();
	}

	synchronized boolean addReplica(ReplicationMessage replicationMessage) throws RGMAPermanentException {
		int currTimeStamp = replicationMessage.getCurrentReplicaTimeStamp();
		int prevTimeStamp = replicationMessage.getPreviousReplicaTimeStamp();
		String hostName = replicationMessage.getHostName();
		boolean fullUpdate = prevTimeStamp == 0;

		int lastReplicationTime = m_registryDatabase.getLastReplicationTimeSecs(hostName);
		if (lastReplicationTime == prevTimeStamp || fullUpdate) {
			if (fullUpdate) {
				Set<UnregisterProducerTableRequest> existingProducers = m_registryDatabase.getProducersFromRemote(hostName);
				for (RegistryProducerResource resource : replicationMessage.getProducers()) {
					m_registryDatabase.addRegistration(resource, false);
					existingProducers.remove(new UnregisterProducerTableRequest(resource.getTableName(), resource.getEndpoint()));
				}
				Set<UnregisterConsumerResourceRequest> existingConsumers = m_registryDatabase.getConsumersFromRemote(hostName);
				for (RegistryConsumerResource resource : replicationMessage.getConsumers()) {
					m_registryDatabase.addRegistration(resource, false);
					existingConsumers.remove(new UnregisterConsumerResourceRequest(resource.getEndpoint()));
				}
				for (UnregisterProducerTableRequest resource : existingProducers) {
					m_registryDatabase.deleteRegistration(resource);
				}
				for (UnregisterConsumerResourceRequest resource : existingConsumers) {
					m_registryDatabase.deleteRegistration(resource);
				}

			} else {
				for (RegistryProducerResource resource : replicationMessage.getProducers()) {
					m_registryDatabase.addRegistration(resource, false);
				}
				for (RegistryConsumerResource resource : replicationMessage.getConsumers()) {
					m_registryDatabase.addRegistration(resource, false);
				}
				for (UnregisterProducerTableRequest resource : replicationMessage.getProducersToDelete()) {
					m_registryDatabase.deleteRegistration(resource);
				}
				for (UnregisterConsumerResourceRequest resource : replicationMessage.getConsumersToDelete()) {
					m_registryDatabase.deleteRegistration(resource);
				}
			}
			/* Note that the timestamp is updated last */
			m_registryDatabase.setLastReplicationTime(replicationMessage.getHostName(), currTimeStamp);
			return true;
		} else if (lastReplicationTime < prevTimeStamp) {
			LOG_REP.warn("Time stamp indicates that a replica message was missed for " + m_vdbName + " from " + hostName + ". Missed interval was "
					+ (prevTimeStamp - lastReplicationTime) + " seconds.");
			return false;
		} else {
			LOG_REP.warn("Time stamp indicates that a replica message appeared " + (lastReplicationTime - prevTimeStamp) + " seconds late for " + m_vdbName
					+ " from " + hostName + ". It will be ignored.");
			return true;
		}
	}

	/**
	 * Get all of the producers currently registered for the table specified
	 * 
	 * @param tableName
	 *            the name of the table that the producer search is done on
	 * @param context
	 *            - security context
	 * @return
	 * @throws RGMATemporaryException
	 * @throws RegistryException
	 * @throws RGMANoWorkingReplicasException
	 *             if this instance is not online
	 */
	List<ProducerTableEntry> getAllProducersForTable(String tableName) throws RGMAPermanentException, RGMATemporaryException {
		checkOnLine();
		return m_registryDatabase.getAllProducersForTable(tableName);
	}

	ReplicationMessage getFullUpdateRecords(int now) throws RGMAPermanentException {
		synchronized (m_replicationTableEntriesLock) {
			if (m_fullUpdateRecords == null) {
				m_fullUpdateRecords = m_registryDatabase.getFullUpdateRecords(now);
			}
			return m_fullUpdateRecords;
		}
	}

	String getVDBName() {
		return m_vdbName;
	}

	boolean isOnline() {
		return m_online;
	}

	/**
	 * shut down this Registry instance.
	 */
	void shutdown() {
		m_timer.cancel();
		m_cleanupThread.shutdown();
		m_online = false;
		m_registryDatabase.shutdown();
		LOG.info("RegistryInstance for VDB " + m_vdbName + " shutdown");
	}

	/**
	 * This returns the current set of replication table entries and initialises the new set. The time stamp must be
	 * updated immediately after calling this to ensure that in the event of any problems a non-matching time stamp will
	 * be sent and a full replica requested.
	 */
	Collection<ReplicationTableEntry> switchReplicaTable() {
		synchronized (m_replicationTableEntriesLock) {
			m_fullUpdateRecords = null;
			Map<String, ReplicationTableEntry> oldTable = m_ReplicationTable;
			m_ReplicationTable = new HashMap<String, ReplicationTableEntry>();
			return oldTable.values();
		}
	}

	void unregisterContinuousConsumer(ResourceEndpoint endpoint) throws RGMAPermanentException, RGMATemporaryException {
		checkOnLine();
		UnregisterConsumerResourceRequest consumer = new UnregisterConsumerResourceRequest(endpoint);
		m_registryDatabase.deleteRegistration(consumer);
		addReplicaEntry(replicaKey(consumer), consumer);
	}

	void unregisterProducerTable(String tableName, ResourceEndpoint endpoint) throws RGMAPermanentException, RGMATemporaryException {
		checkOnLine();
		UnregisterProducerTableRequest producer = new UnregisterProducerTableRequest(tableName, endpoint);
		m_registryDatabase.deleteRegistration(producer);
		addReplicaEntry(replicaKey(producer), producer);
	}

}
