/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.remote;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.registry.RegistryConstants;
import org.glite.rgma.server.servlets.ServletConnection;
import org.glite.rgma.server.servlets.ServletConstants;
import org.glite.rgma.server.system.ConsumerEntry;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSet;

/**
 * Interface to a remote RegistryServlet All RegistryService methods are synchronized to overcome any problems when
 * testing registry connections after a fastest registry failure. By synchronizing each method this protects against the
 * testRegistryConnections() method being called concurrently and having m_theFastestRegistry set to null while it is
 * being accessed/used. m_lock is used to control access to m_theFastestRegistry by the parallel threads used to test
 * registry connections
 */
public class RemoteRegistry {
	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger(RegistryConstants.REGISTRY_LOGGER);

	/**
	 * Converts the result set into a list of ConsumerEntries.
	 */
	private static List<ConsumerEntry> convertToConsumerEntries(TupleSet rs) throws RGMAPermanentException {

		List<ConsumerEntry> consumers = new ArrayList<ConsumerEntry>();
		for (String[] row : rs.getData()) {
			ResourceEndpoint endpoint;
			try {
				endpoint = new ResourceEndpoint(new URL(row[0]), Integer.parseInt(row[1]));
			} catch (MalformedURLException e) {
				throw new RGMAPermanentException("Registry returned invalid URL: " + row[0]);
			}
			ConsumerEntry consumer = new ConsumerEntry(endpoint);
			consumers.add(consumer);
		}
		return consumers;
	}

	/**
	 * Converts the result set into a list of ProducerTableEntries. TODO use 2 functions (probably inlined code) for the
	 * two users of this function and avoid looking at the length of the tuple.
	 */
	private static List<ProducerTableEntry> convertToProducerTableEntries(TupleSet rs) throws RGMAPermanentException {

		List<ProducerTableEntry> producers = new ArrayList<ProducerTableEntry>();

		for (String[] row : rs.getData()) {
			ResourceEndpoint endpoint;
			try {
				endpoint = new ResourceEndpoint(new URL(row[0]), Integer.parseInt(row[1]));
			} catch (MalformedURLException e) {
				throw new RGMAPermanentException("Registry returned invalid URL: " + row[0]);
			}
			ProducerType type = new ProducerType(Boolean.parseBoolean(row[5]), /* isHistory */
			Boolean.parseBoolean(row[6]), /* isLatest */
			Boolean.parseBoolean(row[3]), /* isContinuous */
			Boolean.parseBoolean(row[4]), /* isStatic */
			Boolean.parseBoolean(row[2])); /* isSecondary */

			String vdbName = "";
			String tableName = "";
			if (rs.getData().get(0).length > 9) {
				vdbName = row[9];
				tableName = row[10];
			}

			ProducerTableEntry producer = new ProducerTableEntry(endpoint, vdbName, tableName, type, Integer.parseInt(row[8]), row[7]);
			producers.add(producer);
		}
		return producers;
	}

	private final List<PingThread> m_pingThreads = new ArrayList<PingThread>();

	private Set<URL> m_remotevdbs = new HashSet<URL>();

	/** The fastest registry string */
	private URL m_theFastestRegistry;

	/** to synchronize acces to m_theFastestRegistry */
	private final Object m_theFastestRegistryLock = new Object();

	private String m_vdbName;

	/**
	 * Creates a new RemoteRegistryService object.
	 */
	public RemoteRegistry(String vdbName, Set<URL> urls) {
		m_vdbName = vdbName;
		m_remotevdbs = urls;
	}

	public synchronized List<ProducerTableEntry> getAllProducersForTable(String tableName) throws RGMATemporaryException {
		if (m_theFastestRegistry == null) {
			testRegistryConnections();
		}
		while (m_theFastestRegistry != null) {
			try {
				ServletConnection connection = new ServletConnection(m_theFastestRegistry);
				connection.addParameter(ServletConstants.P_VDB_NAME, m_vdbName);
				connection.addParameter(ServletConstants.P_CAN_FORWARD, false);
				connection.addParameter(ServletConstants.P_TABLE_NAME, tableName);
				TupleSet rs = connection.sendCommand(ServletConstants.M_GET_ALL_PRODUCERS_FOR_TABLE);
				return convertToProducerTableEntries(rs);
			} catch (RemoteException e) {
				LOG.warn("Failed to contact registry at " + m_theFastestRegistry);
				testRegistryConnections();
			} catch (RGMAException e) { /* Though error may be permanent for this one there are others */
				LOG.warn("NoWorkingReplicas found at: " + m_theFastestRegistry);
				testRegistryConnections();
			}
		}
		throw new RGMATemporaryException("Could not find registry service for vdb " + m_vdbName);
	}

	synchronized public String getCurrentRemoteRegistry() {
		if (m_theFastestRegistry == null) {
			return "No fastest registry defined";
		} else {
			return m_theFastestRegistry.toString();
		}
	}

	synchronized public List<ProducerTableEntry> getMatchingProducersForTables(List<String> tables, String predicate, QueryProperties queryProperties,
			boolean isSecondary, ResourceEndpoint consumer, long terminationIntervalSec) throws RGMAPermanentException, RGMATemporaryException {
		String queryType;
		if (queryProperties.isContinuous()) {
			queryType = "continuous";
		} else if (queryProperties.isLatest()) {
			queryType = "latest";
		} else if (queryProperties.isHistory()) {
			queryType = "history";
		} else if (queryProperties.isStatic()) {
			queryType = "static";
		} else {
			throw new RGMAPermanentException("Invalid query properties - not continuous, latest, history or static");
		}
		if (m_theFastestRegistry == null) {
			testRegistryConnections();
		}
		while (m_theFastestRegistry != null) {
			try {
				ServletConnection connection = new ServletConnection(m_theFastestRegistry);
				connection.addParameter(ServletConstants.P_VDB_NAME, m_vdbName);
				connection.addParameter(ServletConstants.P_CAN_FORWARD, false);
				for (int i = 0; i < tables.size(); i++) {
					connection.addParameter("tables", tables.get(i));
				}
				connection.addParameter(ServletConstants.P_TIME_INTERVAL_SEC, 0);
				connection.addParameter(ServletConstants.P_IS_SECONDARY, isSecondary);
				connection.addParameter(ServletConstants.P_PREDICATE, predicate);
				connection.addParameter(ServletConstants.P_QUERY_TYPE, queryType);
				if (consumer != null) {
					connection.addParameter(ServletConstants.P_URL, consumer.getURL().toString());
					connection.addParameter(ServletConstants.P_RESOURCE_ID, consumer.getResourceID());
					connection.addParameter(ServletConstants.P_TERMINATION_INTERVAL_SEC, terminationIntervalSec);
				}
				TupleSet rs = connection.sendCommand(ServletConstants.M_GET_MATCHING_PRODUCERS_FOR_TABLES);
				return convertToProducerTableEntries(rs);
			} catch (RGMAException e) {
				LOG.warn("Registry at " + m_theFastestRegistry + " failed to service request for vdb " + m_vdbName);
				testRegistryConnections();
			} catch (RemoteException e) { /* Though error may be permanent for this one there are others */
				LOG.warn("Failed to contact registry at " + m_theFastestRegistry);
				testRegistryConnections();
			}
		}
		throw new RGMATemporaryException("Could not find registry service for vdb " + m_vdbName);
	}

	synchronized public List<ConsumerEntry> registerProducerTable(ResourceEndpoint producer, String tableName, String predicate, ProducerType producerType,
			int hrpSec, int terminationIntervalSec) throws RGMATemporaryException {
		if (m_theFastestRegistry == null) {
			testRegistryConnections();
		}
		while (m_theFastestRegistry != null) {
			try {
				ServletConnection connection = new ServletConnection(m_theFastestRegistry);
				connection.addParameter(ServletConstants.P_VDB_NAME, m_vdbName);
				connection.addParameter(ServletConstants.P_CAN_FORWARD, false);
				connection.addParameter(ServletConstants.P_URL, producer.getURL().toString());
				connection.addParameter(ServletConstants.P_ID, producer.getResourceID());
				connection.addParameter(ServletConstants.P_TABLE_NAME, tableName);
				connection.addParameter(ServletConstants.P_PREDICATE, predicate);
				connection.addParameter(ServletConstants.P_IS_CONTINUOUS, producerType.isContinuous());
				connection.addParameter(ServletConstants.P_IS_LATEST, producerType.isLatest());
				connection.addParameter(ServletConstants.P_IS_HISTORY, producerType.isHistory());
				connection.addParameter(ServletConstants.P_IS_STATIC, producerType.isStatic());
				connection.addParameter(ServletConstants.P_IS_SECONDARY, producerType.isSecondary());
				connection.addParameter(ServletConstants.P_HRP_SEC, hrpSec);
				connection.addParameter(ServletConstants.P_TERMINATION_INTERVAL_SEC, terminationIntervalSec);
				TupleSet rs = connection.sendCommand(ServletConstants.M_REGISTER_PRODUCER_TABLE);
				return convertToConsumerEntries(rs);
			} catch (RGMAException e) { /* Though error may be permanent for this one there are others */
				LOG.warn("Registry at " + m_theFastestRegistry + " failed to service request for vdb " + m_vdbName);
				testRegistryConnections();
			} catch (RemoteException e) {
				LOG.warn("Failed to contact registry at " + m_theFastestRegistry);
				testRegistryConnections();
			}
		}
		throw new RGMATemporaryException("Could not find registry service for vdb " + m_vdbName);
	}

	synchronized public void unregisterContinuousConsumer(ResourceEndpoint consumer) throws RGMATemporaryException {
		if (m_theFastestRegistry == null) {
			testRegistryConnections();
		}
		while (m_theFastestRegistry != null) {
			try {
				ServletConnection connection = new ServletConnection(m_theFastestRegistry);
				connection.addParameter(ServletConstants.P_VDB_NAME, m_vdbName);
				connection.addParameter(ServletConstants.P_CAN_FORWARD, false);
				connection.addParameter(ServletConstants.P_URL, consumer.getURL().toString());
				connection.addParameter(ServletConstants.P_ID, consumer.getResourceID());
				connection.sendCommand(ServletConstants.M_UNREGISTER_CONTINUOUS_CONSUMER);
				return;
			} catch (RGMAException e) { /* Though error may be permanent for this one there are others */
				LOG.warn("Registry at " + m_theFastestRegistry + " failed to service request for vdb " + m_vdbName);
				testRegistryConnections();
			} catch (RemoteException e) {
				LOG.warn("Failed to contact registry at " + m_theFastestRegistry);
				testRegistryConnections();
			}
		}
		throw new RGMATemporaryException("Could not find registry service for vdb " + m_vdbName);
	}

	synchronized public void unregisterProducerTable(String tableName, ResourceEndpoint producer) throws RGMATemporaryException {
		if (m_theFastestRegistry == null) {
			testRegistryConnections();
		}
		while (m_theFastestRegistry != null) {
			try {
				ServletConnection connection = new ServletConnection(m_theFastestRegistry);
				connection.addParameter(ServletConstants.P_VDB_NAME, m_vdbName);
				connection.addParameter(ServletConstants.P_CAN_FORWARD, false);
				connection.addParameter(ServletConstants.P_TABLE_NAME, tableName);
				connection.addParameter(ServletConstants.P_URL, producer.getURL().toString());
				connection.addParameter(ServletConstants.P_ID, producer.getResourceID());
				connection.sendCommand(ServletConstants.M_UNREGISTER_PRODUCER_TABLE);
				return;
			} catch (RGMAException e) {
				LOG.warn("Registry at " + m_theFastestRegistry + " failed to service request for vdb " + m_vdbName);
				testRegistryConnections();
			} catch (RemoteException e) {
				LOG.warn("Failed to contact registry at " + m_theFastestRegistry);
				testRegistryConnections();
			}
		}
		throw new RGMATemporaryException("Could not find registry service for vdb " + m_vdbName);
	}

	private void setFastestRegistry(URL url) {
		/*
		 * m_theFastestRegistry will be null only if there have been no previous replies so it will be the fastest
		 */
		synchronized (m_theFastestRegistryLock) {
			if (m_theFastestRegistry == null) {
				m_theFastestRegistry = url;
			}
		}
	}

	/**
	 * test the registry connections to see if they work.
	 */
	private void testRegistryConnections() {
		m_theFastestRegistry = null;
		m_pingThreads.clear();
		for (URL url : m_remotevdbs) {
			PingThread thread = new PingThread(url);
			m_pingThreads.add(thread);
		}
		/* All the threads are ready - now start them as close as possible to the same time. */
		for (Thread t : m_pingThreads) {
			t.start();
		}
		/* Wait for m_theFastestRegistry to be set or all threads to complete */
		while (true) {
			synchronized (m_theFastestRegistryLock) {
				if (m_theFastestRegistry != null) {
					break;
				}
			}
			if (m_pingThreads.isEmpty()) {
				break;
			}
			Iterator<PingThread> iter = m_pingThreads.iterator();
			while (iter.hasNext()) {
				if (!iter.next().isAlive()) {
					iter.remove();
				}
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				/* Nothing to do */
			}
		}
		/* Clean up */
		for (Thread t : m_pingThreads) {
			t.interrupt();
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Selected registry for " + m_vdbName + " is now " + m_theFastestRegistry);
		}
	}

	private class PingThread extends Thread {
		private URL m_url = null;

		public PingThread(URL url) {
			m_url = url;
		}

		/* Try at most twice to get a ping through */
		@Override
		public void run() {
			for (int i = 0; i <= 1; i++) {
				try {
					ServletConnection connection = new ServletConnection(m_url);
					connection.addParameter(ServletConstants.P_VDB_NAME, m_vdbName);
					connection.sendCommand(ServletConstants.M_PING);
					setFastestRegistry(m_url);
					return;
				} catch (RGMAException e) {
					LOG.warn("RGMANoWorkingReplicasException when pinging remote registry: " + m_url.getHost() + " for " + m_vdbName + " " + e.getMessage());
				} catch (RemoteException e) {
					LOG.warn("RemoteException when pinging remote registry: " + m_url.getHost() + " for " + m_vdbName + " " + e.getMessage());
				}
			}
		}
	}
}
