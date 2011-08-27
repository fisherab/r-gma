/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.registry;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteRegistry;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.VDBConfigurator;
import org.glite.rgma.server.servlets.ServletResponseReader;
import org.glite.rgma.server.system.ConsumerEntry;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;

public class RegistryService extends Service {

	/*
	 * Note that all public methods are synchronised to ensure that m_vdbs and the Vdbs held there are kept in a
	 * consistent state. There are fancier ways of doing this but the current approach is simple and maybe even
	 * efficient.
	 */

	private class Vdb {
		RegistryInstance m_instance;

		RemoteRegistry m_remoteRegistryService;

		Set<URL> m_remoteURLs;

		RegistryReplicationThread m_replicationThread;

		String m_vdbNameUpper;

		Vdb(String vdbName) {
			m_instance = null;
			m_replicationThread = null;
			m_remoteURLs = null;
			m_remoteRegistryService = null;
			m_vdbNameUpper = vdbName.toUpperCase();
		}

	}

	private static final Logger LOG_REP = Logger.getLogger(RegistryConstants.REGISTRY_REPLICATION_LOGGER);

	private static final Object s_instanceLock = new Object();

	private static RegistryService s_registryService;

	public static void dropInstance() {
		synchronized (s_instanceLock) {
			if (s_registryService != null) {
				synchronized (s_registryService) {
					s_registryService.shutdown();
					s_registryService = null;
				}
			}
		}
	}

	public static RegistryService getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_registryService == null) {
				s_registryService = new RegistryService();
			}
			return s_registryService;
		}
	}

	private ServerConfig m_config;

	private String m_hostname;

	/** Mapping from vdbNameUpper to Vdb */
	private final Map<String, Vdb> m_vdbs = new HashMap<String, Vdb>();

	/**
	 * Registry Server constructor
	 * 
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 */
	private RegistryService() throws RGMAPermanentException {
		super(ServerConstants.REGISTRY_SERVICE_NAME);
		m_logger = Logger.getLogger(RegistryConstants.REGISTRY_LOGGER);
		m_config = getServerConfig();
		m_hostname = getHostname();
		try {
			VDBConfigurator.getInstance().registerRegistryService(this);
		} catch (RGMAPermanentException e) {
			s_controlLogger.fatal("RegistryService failed to start: " + e.getFlattenedMessage());
			throw e;
		}
		s_controlLogger.info("RegistryService started");
	}

	public synchronized boolean addReplica(String replicaXML) throws RGMAPermanentException, RGMATemporaryException {
		String vdbName = null;
		ReplicationMessage message = null;
		try {
			ServletResponseReader reader = new ServletResponseReader();
			List<TupleSet> results = null;
			try {
				results = reader.readResultSets(replicaXML);
			} catch (UnknownResourceException e) {
				throw new RGMAPermanentException(e);
			} catch (NumericException e) {
				throw new RGMAPermanentException(e);
			}
			message = convertResultSetToReplicationMessage(results);
			vdbName = message.getVdbName();
			Vdb vdb = getVdb(vdbName);
			boolean result = vdb.m_instance.addReplica(message);
			if (LOG_REP.isInfoEnabled()) {
				String type = message.getPreviousReplicaTimeStamp() == 0 ? "Full" : "Partial";
				String outcome = result ? "was successful." : "failed";
				LOG_REP.info(type + " update from " + message.getHostName() + " for " + vdbName + " " + message.getCurrentReplicaTimeStamp() + "-"
						+ message.getPreviousReplicaTimeStamp() + " with (P=" + message.getProducers().size() + ", C=" + message.getConsumers().size()
						+ ", UP=" + message.getProducersToDelete().size() + ", UC" + message.getConsumersToDelete().size() + ") " + outcome);
			}
			return result;
		} catch (RGMAPermanentException e) {
			LOG_REP.error("Failed to addReplica to " + vdbName + " " + e.getFlattenedMessage());
			throw e;
		}
	}

	/**
	 * This dynamically sets up a new VDB. If the VDB already exists its display name should be updated if necessary to
	 * match the case and the set of registries updated as necessary. It is possible that a request is received when
	 * there is nothing to do.
	 * 
	 * @throws RGMAPermanentException
	 */
	public synchronized void createVDB(String vdbName, List<URL> registryURLs) throws RGMAPermanentException {
		StringBuilder msg = new StringBuilder("Creating VDB: " + vdbName + " for hosts ");
		for (URL url : registryURLs) {
			msg.append(url.getHost().toString()).append(' ');
		}
		s_controlLogger.debug(msg);

		Set<URL> remoteUrls = new HashSet<URL>(registryURLs);
		boolean needsInstance = false;
		for (URL url : registryURLs) {
			if (url.getHost().equals(m_hostname)) {
				needsInstance = true;
				remoteUrls.remove(url);
			}
		}
		String vdbNameUpper = vdbName.toUpperCase();
		Vdb vdb = m_vdbs.get(vdbNameUpper);
		if (vdb == null) {
			vdb = new Vdb(vdbName);
			m_vdbs.put(vdbNameUpper, vdb);
		}
		boolean remotesExist = remoteUrls.size() > 0;
		boolean remotesChanged = !remoteUrls.equals(vdb.m_remoteURLs);

		if (needsInstance && vdb.m_instance == null) {
			s_controlLogger.debug("It's local and there is no current local vdb for " + vdbName + " so create new instance");
			vdb.m_instance = new RegistryInstance(vdbNameUpper, remotesExist);
		} else if (!needsInstance && vdb.m_instance != null) {
			s_controlLogger.debug("It's not local and there is an instance so shutting down instance " + vdbName);
			vdb.m_instance.shutdown();
			vdb.m_instance = null;
		}

		if (vdb.m_replicationThread != null) {
			if (vdb.m_instance == null || remotesChanged) {
				s_controlLogger.debug("Shutting down replication thread for " + vdbName);
				vdb.m_replicationThread.shutdown();
				vdb.m_replicationThread = null;
			}
		}

		if (vdb.m_replicationThread == null && remotesExist && vdb.m_instance != null) {
			s_controlLogger.debug("Creating replication thread for " + vdbName);
			vdb.m_replicationThread = new RegistryReplicationThread(vdb.m_instance, remoteUrls, m_hostname);
			vdb.m_replicationThread.start();
		}

		if (remotesChanged) {
			if (remotesExist) {
				s_controlLogger.debug("Creating remote registry with " + remoteUrls.size() + " urls for " + vdbName);
				vdb.m_remoteRegistryService = new RemoteRegistry(vdbNameUpper, remoteUrls);
			} else {
				s_controlLogger.debug("Removing remote registry with for " + vdbName);
				vdb.m_remoteRegistryService = null;
			}
			vdb.m_remoteURLs = remoteUrls;
		}

		StringBuffer s = new StringBuffer("Created or updated VDB ");
		s.append(vdbName).append(" for ");
		for (URL u : registryURLs) {
			s.append(u + " ");
		}
		s_controlLogger.info(s);
	}

	/**
	 * This dynamically disables a VDB. The registry on disk is left to be re-used though it will rapidly become out of
	 * date. This is to cope with the case when a VDB is disabled and then immediately created again.
	 */
	public synchronized void disableVDB(String vdbName) {
		String vdbNameUpper = vdbName.toUpperCase();
		Vdb vdb = m_vdbs.get(vdbNameUpper);
		if (vdb != null) {
			if (vdb.m_instance != null) {
				vdb.m_instance.shutdown();
			}
			if (vdb.m_replicationThread != null) {
				vdb.m_replicationThread.shutdown();
			}
			m_vdbs.remove(vdbNameUpper);
		}
		s_controlLogger.info("Disabled VDB " + vdbName);
	}

	/**
	 * get all producers for a table within the named vdb. This is the only real user call on the registry
	 */
	public synchronized List<ProducerTableEntry> getAllProducersForTable(String tableName, boolean canForward, String vdbName) throws RGMAPermanentException,
			RGMATemporaryException, RGMAPermanentException {
		checkOnline();
		String vdbNameUpper = vdbName.toUpperCase();
		if (vdbName.equals("")) {
			vdbName = vdbNameUpper = "DEFAULT";
		}
		Vdb vdb = m_vdbs.get(vdbNameUpper);
		if (vdb == null) {
			throw new RGMAPermanentException("VDB: " + vdbName + " is not recognised by this server");
		}
		List<ProducerTableEntry> producers = new LinkedList<ProducerTableEntry>();
		if (vdb.m_instance != null) {
			try {
				producers = vdb.m_instance.getAllProducersForTable(tableName);
			} catch (RGMAPermanentException e) {
				if (m_logger.isInfoEnabled()) {
					StringBuilder msg = new StringBuilder("Problem occurred getAllProducersForTable for VDB: ");
					msg.append(vdbName).append(" ").append(e.getFlattenedMessage());
					m_logger.info(msg);
				}
				throw e;
			} catch (RGMATemporaryException e) {
				if (m_logger.isInfoEnabled()) {
					StringBuilder msg = new StringBuilder("Problem occurred getAllProducersForTable for VDB: ");
					msg.append(vdbName);
					msg.append(" ");
					msg.append(e.getFlattenedMessage());
					m_logger.info(msg);
				}
				throw e;
			}
		} else if (canForward) {
			try {
				producers = vdb.m_remoteRegistryService.getAllProducersForTable(tableName);
			} catch (RGMATemporaryException e) {
				if (m_logger.isInfoEnabled()) {
					m_logger.info("Problem occurred remote getAllProducersForTable for VDB: " + vdbName + " " + e.getFlattenedMessage());
				}
				throw e;
			}
		} else {
			throw new RGMAPermanentException("Forwarding of VDB: " + vdbName + " is not permitted");
		}

		if (m_logger.isInfoEnabled()) {
			StringBuilder msg = new StringBuilder("Returning ");
			msg.append(producers.size());
			msg.append(" producers for table ");
			msg.append(tableName);
			msg.append(" in vdb \"");
			msg.append(vdbName);
			msg.append("\" from getAllProducersForTable");
			m_logger.info(msg);
		}
		return producers;
	}

	public synchronized List<ProducerTableEntry> getMatchingProducersForTables(String vdbName, boolean canForward, List<String> tableNames, String predicate,
			QueryProperties queryProperties, boolean isSecondary, ResourceEndpoint consumerEndpoint, int terminationIntervalSecs)
			throws RGMAPermanentException, RGMATemporaryException {
		if (m_logger.isDebugEnabled()) {
			StringBuilder msg = new StringBuilder("RegistryService:getMatchingProducersForTables called with  vdbName=" + vdbName + " canForward=" + canForward
					+ " predicate=" + predicate + " queryProperties= " + queryProperties + " isSecondary=" + isSecondary);
			if (consumerEndpoint != null) {
				msg.append(" consumerEndpoint=" + consumerEndpoint + " terminationIntervalSecs=" + terminationIntervalSecs);
			}
			for (String tableName : tableNames) {
				msg.append(" tableName=");
				msg.append(tableName);
			}
			m_logger.debug(msg.toString());
		}
		List<ProducerTableEntry> producers = new LinkedList<ProducerTableEntry>();
		try {
			if (tableNames.size() == 0) {
				throw new RGMAPermanentException("List of table names may not be empty");
			}
			checkOnline();
			Vdb vdb = getVdb(vdbName);

			/*
			 * See if it is a valid consumer predicate if its not valid it should not be registered. The following call
			 * will throw an exception otherwise.
			 */
			new ConsumerPredicate(predicate);

			RegistryInstance reg = vdb.m_instance;
			if (reg != null) {
				producers = reg.getMatchingProducersForTables(tableNames, predicate, queryProperties, isSecondary, consumerEndpoint, terminationIntervalSecs);
			} else if (canForward) {
				producers = vdb.m_remoteRegistryService.getMatchingProducersForTables(tableNames, predicate, queryProperties, isSecondary, consumerEndpoint,
						terminationIntervalSecs);
			} else {
				throw new RGMAPermanentException("Forwarding of VDB: " + vdbName + " is not permitted");
			}
		} catch (RGMAPermanentException e) {
			m_logger.error("Problem getMatchingProducersForTable for VDB: " + vdbName + " " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.error("Problem getMatchingProducersForTable for VDB: " + vdbName + " " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			StringBuilder msg = new StringBuilder("getMatchingProducersForTables returns ");
			msg.append(producers.size());
			msg.append(" producers for table ");
			for (String table : tableNames) {
				msg.append(vdbName).append('.').append(table).append(' ');
			}
			m_logger.info(msg);
		}
		return producers;
	}

	private void appendDetailsForVdb(Vdb vdb, StringBuilder xml) {
		xml.append("<VDB ID=\"").append(vdb.m_vdbNameUpper);
		String location = null;
		if (vdb.m_instance != null) {
			location = "local";
			try {
				xml.append("\" masterRegisteredConsumers=\"").append(vdb.m_instance.getRegisteredConsumerCount(true));
			} catch (RGMAPermanentException e) {
				xml.append("\" masterRegisteredConsumers=\"RGMAPermanentException ").append(e.getFlattenedMessage());
			}
			try {
				xml.append("\" masterRegisteredProducers=\"").append(vdb.m_instance.getRegisteredProducerCount(true));
			} catch (RGMAPermanentException e) {
				xml.append("\" masterRegisteredProducers=\"RGMAPermanentException ").append(e.getFlattenedMessage());
			}
			try {
				xml.append("\" totalRegisteredConsumers=\"").append(vdb.m_instance.getRegisteredConsumerCount(false));
			} catch (RGMAPermanentException e) {
				xml.append("\" totalRegisteredConsumers=\"RGMAPermanentException ").append(e.getFlattenedMessage());
			}
			try {
				xml.append("\" totalRegisteredProducers=\"").append(vdb.m_instance.getRegisteredProducerCount(false));
			} catch (RGMAPermanentException e) {
				xml.append("\" totalRegisteredProducers=\"RGMAPermanentException ").append(e.getFlattenedMessage());
			}
		}
		if (vdb.m_remoteURLs != null) {
			xml.append("\" remoteVDBCount=\"").append(vdb.m_remoteURLs.size());
			if (vdb.m_instance == null) {
				location = vdb.m_remoteRegistryService.getCurrentRemoteRegistry();
			}
		} else {
			xml.append("\" remoteVDBCount=\"").append(0);
		}
		xml.append("\" location=\"").append(location).append("\">");
	}

	public synchronized String getProperty(String name, String param) throws RGMAPermanentException, RGMAPermanentException {
		StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
		if (name.equalsIgnoreCase(ServerConstants.SERVICE_RESOURCES)) {
			xml.append("<Registry ReplicationIntervalMillis=\"").append(
					Long.parseLong(m_config.getProperty(ServerConstants.REGISTRY_REPLICATION_INTERVAL_SECS)) * 1000).append("\">");
			for (Vdb vdb : m_vdbs.values()) {
				appendDetailsForVdb(vdb, xml);
				xml.append("</VDB>");
			}
			xml.append("</Registry>");

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_STATUS)) {
			if (param == null) {
				throw new RGMAPermanentException("Required parameter missing: parameter" + param);
			}
			Vdb vdb = m_vdbs.get(param);
			if (vdb != null) {
				xml.append("<Registry>");
				appendDetailsForVdb(vdb, xml);
				if (vdb.m_instance != null) {
					if (vdb.m_replicationThread != null) {
						String details = vdb.m_replicationThread.getReplicationDetails();
						if (details != null) {
							xml.append(details);
						}
					}
					xml.append("<UniqueTableNames>");
					for (String s : vdb.m_instance.getUniqueTableNames()) {
						xml.append("<Name>").append(s).append("</Name>");
					}
					xml.append("</UniqueTableNames>");
					xml.append("<UniqueHostNames>");
					for (String s : vdb.m_instance.getUniqueHostNames()) {
						xml.append("<Name>").append(s).append("</Name>");
					}
					xml.append("</UniqueHostNames>");
				} else {
					for (URL url : vdb.m_remoteURLs) {
						xml.append("<RemoteVDB URL=\"").append(url).append("\"/>");
					}
				}
				xml.append("</VDB></Registry>");
			} else {
				throw new RGMAPermanentException("Unknown VDB: " + param);
			}

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_TABLE_STATUS)) {
			if (param == null) {
				throw new RGMAPermanentException("Parameter missing: " + param);
			}
			int spacePos = param.indexOf(' ');
			if (spacePos < 0) {
				throw new RGMAPermanentException("Parameter " + param + " must contain a separator space");
			}
			String vdbName = param.substring(0, spacePos);
			String tableName = param.substring(spacePos + 1);
			Vdb vdb = m_vdbs.get(vdbName);
			if (vdb == null) {
				throw new RGMAPermanentException("Unknown VDB: " + vdbName);
			}
			xml.append("<Registry><VDB ID=\"").append(vdb.m_vdbNameUpper).append("\">");
			xml.append("<ProducerTableEntriesForTableName ID=\"").append(tableName).append("\">");
			xml.append(vdb.m_instance.getProducerTableEntriesForTableName(tableName));
			xml.append("</ProducerTableEntriesForTableName>");
			xml.append("<ConsumerTableEntriesForTableName ID=\"").append(tableName).append("\">");
			xml.append(vdb.m_instance.getConsumerTableEntriesForTableName(tableName));
			xml.append("</ConsumerTableEntriesForTableName>");
			xml.append("</VDB></Registry>");

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_HOST_STATUS)) {
			if (param == null) {
				throw new RGMAPermanentException("Parameter missing: " + param);
			}
			int spacePos = param.indexOf(' ');
			if (spacePos < 0) {
				throw new RGMAPermanentException("Parameter " + param + " must contain a separator space");
			}
			String vdbName = param.substring(0, spacePos);
			String hostName = param.substring(spacePos + 1);
			Vdb vdb = m_vdbs.get(vdbName);
			if (vdb == null) {
				throw new RGMAPermanentException("Unknown VDB: " + vdbName);
			}
			xml.append("<Registry><VDB ID=\"").append(vdb.m_vdbNameUpper).append("\">");
			xml.append("<ProducerTableEntriesForHostName ID=\"").append(hostName).append("\">");
			xml.append(vdb.m_instance.getProducerTableEntriesForHostName(hostName));
			xml.append("</ProducerTableEntriesForHostName>");
			xml.append("<ConsumerTableEntriesForHostName ID=\"").append(hostName).append("\">");
			xml.append(vdb.m_instance.getConsumerTableEntriesForHostName(hostName));
			xml.append("</ConsumerTableEntriesForHostName>");
			xml.append("</VDB></Registry>");
		} else {
			xml.append(getServiceProperty(name, param));
		}
		return xml.toString();
	}

	public synchronized void ping(String vdbName) throws RGMAPermanentException, RGMATemporaryException, RGMAPermanentException {
		checkOnline();
		Vdb vdb = getVdb(vdbName);
		if (vdb.m_instance != null) {
			if (!vdb.m_instance.isOnline()) {
				throw new RGMATemporaryException("No replica online for VDB: " + vdbName);
			}
		} else {
			throw new RGMAPermanentException("No VDB named " + vdbName + " locally.");
		}
	}

	public synchronized List<ConsumerEntry> registerProducerTable(String vdbName, boolean canForward, ResourceEndpoint producer, String tableName,
			String predicate, ProducerType producerType, int hrpSec, int terminationIntervalSec) throws RGMAPermanentException, RGMAPermanentException,
			RGMATemporaryException {
		List<ConsumerEntry> consumers = new LinkedList<ConsumerEntry>();
		try {
			checkOnline();
			Vdb vdb = getVdb(vdbName);
			RegistryInstance reg = vdb.m_instance;
			if (reg != null) {
				consumers = reg.registerProducerTable(producer, tableName, predicate, producerType, hrpSec, terminationIntervalSec);
			} else if (canForward) {
				consumers = vdb.m_remoteRegistryService.registerProducerTable(producer, tableName, predicate, producerType, hrpSec, terminationIntervalSec);
			} else {
				throw new RGMAPermanentException("Forwarding of VDB: " + vdbName + " is not permitted");
			}
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to registerProducerTable " + vdbName + "." + tableName + " " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to registerProducerTable " + vdbName + "." + tableName + " " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("registerProducerTable returned " + consumers.size() + " consumers for table " + vdbName + "." + tableName + " " + producer
					+ producerType + " pred=" + predicate + " hrp=" + hrpSec + " TI=" + terminationIntervalSec);
		}
		return consumers;
	}

	public synchronized void unregisterContinuousConsumer(String vdbName, boolean canForward, ResourceEndpoint consumer) throws RGMATemporaryException,
			RGMAPermanentException {
		try {
			checkOnline();
			Vdb vdb = getVdb(vdbName);
			RegistryInstance reg = vdb.m_instance;
			if (reg != null) {
				reg.unregisterContinuousConsumer(consumer);
			} else if (canForward) {
				vdb.m_remoteRegistryService.unregisterContinuousConsumer(consumer);
			} else {
				throw new RGMAPermanentException("Forwarding of VDB: " + vdbName + " is not permitted");
			}
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to unregisterContinuousConsumer " + consumer + " for " + vdbName + " " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.error("Failed to unregisterContinuousConsumer " + consumer + " for " + vdbName + " " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("unregisterContinuousConsumer " + consumer + " for " + vdbName);
		}
	}

	public synchronized void unregisterProducerTable(String vdbName, boolean canForward, String tableName, ResourceEndpoint producer)
			throws RGMAPermanentException, RGMATemporaryException, RGMAPermanentException {
		try {
			checkOnline();
			Vdb vdb = getVdb(vdbName);
			RegistryInstance reg = vdb.m_instance;
			if (reg != null) {
				reg.unregisterProducerTable(tableName, producer);
			} else if (canForward) {
				vdb.m_remoteRegistryService.unregisterProducerTable(tableName, producer);
			} else {
				throw new RGMAPermanentException("Forwarding of VDB: " + vdbName + " is not permitted");
			}
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to unregisterProducerTable " + vdbName + "." + tableName + " " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.error("Failed to unregisterProducerTable " + vdbName + "." + tableName + " " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("unregisteredProducerTable " + vdbName + "." + tableName + " from " + producer);
		}
	}

	private ReplicationMessage convertResultSetToReplicationMessage(List<TupleSet> rs) throws RGMAPermanentException {
		try {
			List<RegistryProducerResource> ps = new ArrayList<RegistryProducerResource>();
			List<RegistryConsumerResource> cs = new ArrayList<RegistryConsumerResource>();
			List<UnregisterProducerTableRequest> ups = new ArrayList<UnregisterProducerTableRequest>();
			List<UnregisterConsumerResourceRequest> ucs = new ArrayList<UnregisterConsumerResourceRequest>();
			String vdbName = null;
			String hostName = null;
			int currentReplicaTimeStamp = 0;
			int previousReplicaTimeStamp = 0;

			if (rs.size() != 5) {
				throw new RGMAPermanentException("Registry replication message must contain 5 tables");
			}

			/*
			 * Note that the order of the tables here must match that in
			 * RegistryReplicationThread.convertReplicaMessageToResultSetList
			 */
			int tableNum = 0;

			TupleSet r = rs.get(tableNum++); /* replicationHeader */
			String[] header = r.getData().get(0);
			vdbName = header[0];
			hostName = header[1];
			currentReplicaTimeStamp = Integer.parseInt(header[2]);
			previousReplicaTimeStamp = Integer.parseInt(header[3]);

			r = rs.get(tableNum++); /* producers */
			for (String[] row : r.getData()) {
				ResourceEndpoint endpoint = new ResourceEndpoint(new URL(row[0]), Integer.parseInt(row[1]));
				ProducerType type = new ProducerType(Boolean.parseBoolean(row[4]), Boolean.parseBoolean(row[5]), Boolean.parseBoolean(row[6]), Boolean
						.parseBoolean(row[7]), Boolean.parseBoolean(row[8]));
				RegistryProducerResource resource = new RegistryProducerResource(row[10], endpoint, row[2], Integer.parseInt(row[3]), type, Integer
						.parseInt(row[9]));
				ps.add(resource);
			}

			r = rs.get(tableNum++); /* consumers */
			for (String[] row : r.getData()) {
				ResourceEndpoint endpoint = new ResourceEndpoint(new URL(row[0]), Integer.parseInt(row[1]));
				QueryProperties props = null;
				if (Boolean.parseBoolean(row[4])) {
					props = QueryProperties.HISTORY;
				} else if (Boolean.parseBoolean(row[5])) {
					props = QueryProperties.LATEST;
				} else if (Boolean.parseBoolean(row[6])) {
					props = QueryProperties.CONTINUOUS;
				} else if (Boolean.parseBoolean(row[7])) {
					props = QueryProperties.STATIC;
				} else {
					throw new RGMAPermanentException("Consumer with no QueryProperties");
				}
				RegistryConsumerResource resource = new RegistryConsumerResource(row[9], endpoint, row[2], Integer.parseInt(row[3]), props, Boolean
						.parseBoolean(row[8]));
				cs.add(resource);
			}

			r = rs.get(tableNum++); /* producersToDelete */
			for (String[] row : r.getData()) {
				ResourceEndpoint endpoint = new ResourceEndpoint(new URL(row[0]), Integer.parseInt(row[1]));
				UnregisterProducerTableRequest resource = new UnregisterProducerTableRequest(row[2], endpoint);
				ups.add(resource);
			}

			r = rs.get(tableNum++); /* consumersToDelete */
			for (String[] row : r.getData()) {
				ResourceEndpoint endpoint = new ResourceEndpoint(new URL(row[0]), Integer.parseInt(row[1]));
				UnregisterConsumerResourceRequest resource = new UnregisterConsumerResourceRequest(endpoint);
				ucs.add(resource);
			}

			return new ReplicationMessage(vdbName, hostName, currentReplicaTimeStamp, previousReplicaTimeStamp, ps, cs, ups, ucs);

		} catch (MalformedURLException e) {
			throw new RGMAPermanentException(e);
		}
	}

	private Vdb getVdb(String vdbName) throws RGMAPermanentException {
		String vdbNameUpper = vdbName.toUpperCase();
		if (vdbName.equals("")) {
			vdbName = vdbNameUpper = "DEFAULT";
		}
		Vdb vdb = m_vdbs.get(vdbNameUpper);
		if (vdb == null) {
			throw new RGMAPermanentException("VDB: " + vdbName + " is not recognised by this server");
		}
		return vdb;
	}

	/**
	 * shutdown this service.
	 */
	private void shutdown() {
		setOffline("Shutting down");
		for (Vdb vdb : m_vdbs.values()) {
			if (vdb.m_replicationThread != null) {
				vdb.m_replicationThread.shutdown();
			}
			if (vdb.m_instance != null) {
				vdb.m_instance.shutdown();
			}
		}
		s_registryService = null;
		s_controlLogger.info("RegistryService shutdown");
	}
}
