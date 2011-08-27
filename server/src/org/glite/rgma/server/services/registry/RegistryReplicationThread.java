/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.registry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.tasks.Task;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.servlets.ServletConnection;
import org.glite.rgma.server.servlets.ServletConstants;
import org.glite.rgma.server.servlets.ServletResponseReader;
import org.glite.rgma.server.servlets.ServletResponseWriter;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;

/**
 * Thread that will push a set of updates to all other registry instances every service defined replication period
 */
class RegistryReplicationThread extends Thread {
	private static final Logger LOG = Logger.getLogger(RegistryConstants.REGISTRY_REPLICATION_LOGGER);

	/**
	 * The registry database
	 */
	private RegistryInstance m_registryInstance;

	/**
	 * The time interval before pushing a set of updates
	 */
	private int m_replicationInterval;

	private boolean m_shutdown = false;

	private boolean m_initialised = false;

	private TaskManager m_manager = null;

	private Set<URL> m_remoteReplicas = null;

	private String m_ownerID = null;

	private String m_hostname = null;

	private HashMap<String, RegistryReplicationSender> m_senderTasks = new HashMap<String, RegistryReplicationSender>();

	private int m_lastReplicationTime;

	private long m_maxTaskTime;

	/**
	 * This is the thread that will periodically push replicas
	 * 
	 * @param registryDatabase
	 * @throws RGMAPermanentException
	 */
	RegistryReplicationThread(RegistryInstance registryInstance, Set<URL> remoteReplicas, String hostname) throws RGMAPermanentException {
		m_registryInstance = registryInstance;
		m_remoteReplicas = remoteReplicas;
		m_manager = TaskManager.getInstance();
		m_hostname = hostname;
		m_replicationInterval = Integer.parseInt(ServerConfig.getInstance().getProperty(ServerConstants.REGISTRY_REPLICATION_INTERVAL_SECS));
		m_ownerID = RegistryConstants.REGISTRY_REPLICATION_THREAD + m_registryInstance.getVDBName();
		m_maxTaskTime = ServerConfig.getInstance().getLong(ServerConstants.REGISTRY_REPLICATION_MAX_TASK_TIME_SECS) * 1000;
	}

	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Starting registry replication thread for VDB " + m_registryInstance.getVDBName() + " replication interval is " + m_replicationInterval
					+ " seconds");
		}
		while (!m_shutdown) {
			Collection<ReplicationTableEntry> entries = m_registryInstance.switchReplicaTable();
			int now = (int) (System.currentTimeMillis() / 1000);
			int lastReplicationTime = m_lastReplicationTime;
			m_lastReplicationTime = now;

			ReplicationMessage replicaMessage;
			if (lastReplicationTime != 0) {
				List<RegistryProducerResource> ps = new ArrayList<RegistryProducerResource>();
				List<RegistryConsumerResource> cs = new ArrayList<RegistryConsumerResource>();
				List<UnregisterProducerTableRequest> ups = new ArrayList<UnregisterProducerTableRequest>();
				List<UnregisterConsumerResourceRequest> ucs = new ArrayList<UnregisterConsumerResourceRequest>();
				for (ReplicationTableEntry entry : entries) {
					if (entry instanceof RegistryProducerResource) {
						ps.add((RegistryProducerResource) entry);
					}
					if (entry instanceof RegistryConsumerResource) {
						cs.add((RegistryConsumerResource) entry);
					}
					if (entry instanceof UnregisterProducerTableRequest) {
						ups.add((UnregisterProducerTableRequest) entry);
					}
					if (entry instanceof UnregisterConsumerResourceRequest) {
						ucs.add((UnregisterConsumerResourceRequest) entry);
					}
				}
				replicaMessage = new ReplicationMessage(m_registryInstance.getVDBName(), m_hostname, now, lastReplicationTime, ps, cs, ups, ucs);
			} else {
				/* First time so force a full update */
				replicaMessage = null;
			}

			for (URL url : m_remoteReplicas) {
				/* Make sure that any old task is aborted */
				synchronized (m_senderTasks) {
					RegistryReplicationSender sndr = m_senderTasks.remove(url.getHost());
					if (sndr != null) {
						sndr.abort();
					}
				}
				RegistryReplicationSender sender = new RegistryReplicationSender(m_ownerID, url.getHost() + "Registry", url, replicaMessage, m_maxTaskTime, 2);
				m_manager.add(sender);
				synchronized (m_senderTasks) {
					m_senderTasks.put(url.getHost(), sender);
				}
			}
			m_initialised = true;
			/* Have a sleep before its done again */
			try {
				Thread.sleep(m_replicationInterval * 1000);
			} catch (InterruptedException e) {
				/* Nothing to do */
			}
		}
	}

	String getReplicationDetails() {
		StringBuilder xml = new StringBuilder();
		synchronized (m_senderTasks) {
			for (URL url : m_remoteReplicas) {

				xml.append("<Replica URL=\"").append(url);

				xml.append("\" lastIncomingReplicationTimeMillis=\"");
				try {
					if (m_registryInstance.getLastReplicationTime(url.getHost()) == 0) {
						xml.append("Not replicated yet.");
					} else {
						xml.append(m_registryInstance.getLastReplicationTime(url.getHost()) * 1000L);
					}
				} catch (RGMAPermanentException e) {
					xml.append("RGMAPermanentException " + e);
				}
				xml.append("\" lastOutgoingReplicationIntervalMillis=\"");
				long lastRepTime = m_lastReplicationTime * 1000L;
				if (lastRepTime == 0) {
					xml.append("Not replicated yet.");
				} else {
					xml.append(System.currentTimeMillis() - lastRepTime);
				}
				xml.append("\">");

				if (m_senderTasks.get(url.getHost()) != null) {
					Map<String, String> taskDetails = m_senderTasks.get(url.getHost()).statusInfo();
					xml.append("<Task Key=\"").append(taskDetails.get("Key"));
					xml.append("\" CreationTimeMillis=\"").append(taskDetails.get("CreationTimeMillis"));
					xml.append("\" MaxAttemptCount=\"").append(taskDetails.get("MaxAttemptCount"));
					xml.append("\" MaxRunTimeMillis=\"").append(taskDetails.get("MaxRunTimeMillis"));
					xml.append("\" Owner=\"").append(taskDetails.get("Owner"));
					xml.append("\" Attempts=\"").append(taskDetails.get("Attempts"));
					xml.append("\" ResultCode=\"").append(taskDetails.get("ResultCode"));
					xml.append("\"/>");
				}
				xml.append("</Replica>");
			}
		}
		return xml.toString();
	}

	/**
	 * Has this thread been through at least one cleanup cycle
	 * 
	 * @return true if this thread has been through at least one cleanup cycle
	 */
	boolean isInitialised() {
		return m_initialised;
	}

	/**
	 * Shutdown this thread
	 */
	void shutdown() {
		LOG.info("Shutdown received by registry vdb, setting shutdown to true");
		m_shutdown = true;
		interrupt();
	}

	String convertResultSetListToXML(List<TupleSet> rslist) throws RGMAPermanentException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ServletResponseWriter writer = new ServletResponseWriter(new PrintWriter(baos));
		try {
			writer.writeResultSets(rslist);
			writer.close();
		} catch (IOException e) {
			LOG.warn("Error occurred converting result set to xml " + e.getMessage());
			throw new RGMAPermanentException("Error occured convertion result set to XML: " + e.getMessage());
		}
		return baos.toString();
	}

	List<TupleSet> convertReplicaMessageToResultSetList(ReplicationMessage msg) throws RGMAPermanentException {

		/*
		 * Note that the order of the tables here must match that in
		 * RegistryService.convertResultSetToReplicationMessage
		 */
		List<TupleSet> resultSetList = new ArrayList<TupleSet>();

		/* replicationHeader */
		TupleSet rs = new TupleSet();
		String[] replicationDataRow = { msg.getVdbName(), msg.getHostName(), msg.getCurrentReplicaTimeStamp() + "", msg.getPreviousReplicaTimeStamp() + "" };
		rs.addRow(replicationDataRow);
		resultSetList.add(rs);

		/* producers */
		rs = new TupleSet();
		for (RegistryProducerResource producer : msg.getProducers()) {
			String url = producer.getEndpoint().getURL().toString();
			String id = producer.getEndpoint().getResourceID() + "";
			String predicate = producer.getPredicate();
			String terminationIntervalSecs = producer.getTerminationIntervalSecs() + "";
			String isHistory = producer.isHistory() ? "true" : "false";
			String isLatest = producer.isLatest() ? "true" : "false";
			String isContinuous = producer.isContinuous() ? "true" : "false";
			String isStatic = producer.isStatic() ? "true" : "false";
			String isSecondary = producer.isSecondary() ? "true" : "false";
			String hrpSecs = producer.getHrpSecs() + "";
			String tableName = producer.getTableName();
			String[] producerRow = new String[] { url, id, predicate, terminationIntervalSecs, isHistory, isLatest, isContinuous, isStatic, isSecondary,
					hrpSecs, tableName };
			rs.addRow(producerRow);
		}
		resultSetList.add(rs);

		/* consumers */
		rs = new TupleSet();
		for (RegistryConsumerResource consumer : msg.getConsumers()) {
			String url = consumer.getEndpoint().getURL().toString();
			String id = Integer.toString(consumer.getEndpoint().getResourceID());
			String predicate = consumer.getPredicate();
			String terminationIntervalSecs = Integer.toString(consumer.getTerminationIntervalSecs());
			String isHistory = consumer.isHistory() ? "true" : "false";
			String isLatest = consumer.isLatest() ? "true" : "false";
			String isContinuous = consumer.isContinuous() ? "true" : "false";
			String isStatic = consumer.isStatic() ? "true" : "false";
			String isSecondary = consumer.isSecondary() ? "true" : "false";
			String tableName = consumer.getTableName();
			String[] consumerRow = new String[] { url, id, predicate, terminationIntervalSecs, isHistory, isLatest, isContinuous, isStatic, isSecondary,
					tableName };
			rs.addRow(consumerRow);
		}
		resultSetList.add(rs);

		/* producersToDelete */
		rs = new TupleSet();
		for (UnregisterProducerTableRequest producer : msg.getProducersToDelete()) {
			String url = producer.getEndpoint().getURL().toString();
			String id = producer.getEndpoint().getResourceID() + "";
			String tableName = producer.getTableName();
			String[] producerRow = new String[] { url, id, tableName };
			rs.addRow(producerRow);
		}
		resultSetList.add(rs);

		/* consumersToDelete */
		rs = new TupleSet();
		for (UnregisterConsumerResourceRequest consumer : msg.getConsumersToDelete()) {
			String url = consumer.getEndpoint().getURL().toString();
			String id = consumer.getEndpoint().getResourceID() + "";
			String[] producerRow = new String[] { url, id };
			rs.addRow(producerRow);
		}
		resultSetList.add(rs);

		return resultSetList;
	}

	private class RegistryReplicationSender extends Task {
		URL m_target = null;

		ReplicationMessage m_replicationMessage;

		public RegistryReplicationSender(String ownerId, String key, URL target, ReplicationMessage replicationMessage, long maxRunTimeMillis,
				int maxAttemptCount) {
			super(ownerId, key, maxRunTimeMillis, maxAttemptCount);
			m_target = target;
			m_replicationMessage = replicationMessage;
		}

		public Result invoke() {
			try {
				if (m_replicationMessage != null && doUpdate(m_replicationMessage)) {
					LOG.debug("Partial update " + m_replicationMessage + " accepted by " + m_target.getHost());
					return Result.SUCCESS;
				} else {
					/* m_lastReplicationTime is already set to to current time */
					m_replicationMessage = m_registryInstance.getFullUpdateRecords(m_lastReplicationTime);
					boolean success = doUpdate(m_replicationMessage);
					if (LOG.isDebugEnabled()) {
						if (success) {
							LOG.debug("Full update " + m_replicationMessage + " accepted by " + m_target.getHost());
						} else {
							LOG.debug("Full update " + m_replicationMessage + " rejected by " + m_target.getHost());
						}
					}
					return Result.SUCCESS;
				}
			} catch (RemoteException e) {
				LOG.warn("Unexpected RemoteException " + e.getMessage() + " sending to " + m_target.getHost());
				return Result.SOFT_ERROR;
			} catch (RGMAPermanentException e) {
				LOG.error("Unexpected RGMAPermanentException " + e.getFlattenedMessage());
				return Result.HARD_ERROR;
			} catch (Throwable t) {
				LOG.error("Unexpected Throwable", t);
				return Result.HARD_ERROR;
			}
		}

		private boolean doUpdate(ReplicationMessage replicaMessage) throws RemoteException, RGMAPermanentException, RGMATemporaryException {
			try {
				ServletConnection connection = new ServletConnection(m_target, ServletConnection.HttpMethod.POST);
				connection.addParameter(ServletConstants.P_REPLICA, convertResultSetListToXML(convertReplicaMessageToResultSetList(replicaMessage)));
				String response = connection.sendXMLCommand("addReplica");
				ServletResponseReader reader = new ServletResponseReader();
				TupleSet rs = reader.readResultSet(response);
				String status = rs.getData().get(0)[0];
				return status.equals("OK");
			} catch (UnknownResourceException e) {
				throw new RGMAPermanentException(e);
			} catch (NumericException e) {
				throw new RGMAPermanentException(e);
			}
		}
	}
}
