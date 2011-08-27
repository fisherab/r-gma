/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.schema;

import java.net.URL;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteSchema;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.tasks.Task;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.services.tasks.Task.Result;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.SchemaIndex;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UserContextInterface;

public class SchemaInstance {

	private class ReplicateTask extends Task {

		public ReplicateTask() {
			super(m_serviceURL + m_vdbNameUpper, m_masterURL.toString(), s_replicationTaskMaxTimeMillis, s_schemaMaxTaskAttemptCount);
		}

		public Result invoke() {
			try {
				getAndPutReplica();
				return Result.SUCCESS;
			} catch (RGMAPermanentException e) {
				LOG.error("ReplicateTask got RGMAPermanentException for " + m_vdbNameUpper + " " + e.getFlattenedMessage());
				return Result.HARD_ERROR;
			} catch (RGMATemporaryException e) {
				LOG.warn("ReplicateTask got RGMANoWorkingReplicasException for " + m_vdbNameUpper + " " + e.getFlattenedMessage());
				return Result.SOFT_ERROR;
			} catch (RemoteException e) {
				LOG.warn("ReplicateTask got RemoteException for " + m_vdbNameUpper + " " + e.getMessage());
				return Result.SOFT_ERROR;
			} catch (Throwable t) {
				LOG.error("Unexpected error in schema replication for " + m_vdbNameUpper, t);
				return Result.HARD_ERROR;
			}
		}
	}

	private class ReplicateTimerTask extends TimerTask {

		@Override
		public void run() {
			if (m_replicateTask != null) {
				Result resultCode = m_replicateTask.getResultCode();
				if (resultCode != Result.SUCCESS) {
					synchronized (m_replicationStatus) {
						m_replicationStatus.m_lastError = "Replication result is " + resultCode;
						m_replicationStatus.m_lastErrorTime = System.currentTimeMillis();
					}
				} else {
					synchronized (m_replicationStatus) {
						m_replicationStatus.m_lastSuccessTime = System.currentTimeMillis();
					}
				}
				m_replicateTask.abort();
			}
			m_replicateTask = new ReplicateTask();
			s_taskManager.add(m_replicateTask);
		}
	}

	private class ReplicationStatus {
		public long m_lastSuccessTime;

		public String m_lastError;

		public long m_lastErrorTime;
	}

	private final static Logger LOG = Logger.getLogger(SchemaConstants.SCHEMA_LOGGER);

	private static int s_replicationIntervalMillis;

	private static long s_replicationTaskMaxTimeMillis;

	private static int s_schemaMaxTaskAttemptCount;

	private static TaskManager s_taskManager;

	// the url of the master of schema instance for the VDB
	private URL m_masterURL;

	private Task m_replicateTask;

	// Used to access the schema database
	private SchemaDatabase m_schemaDatabase;

	private URL m_serviceURL;

	private Timer m_Timer;

	// the namespace of the virtual database that this instance represents
	private String m_vdbNameUpper = null;

	private ReplicationStatus m_replicationStatus;

	public static void setStaticVariables() throws RGMAPermanentException {
		s_taskManager = TaskManager.getInstance();
		ServerConfig serverConfig = ServerConfig.getInstance();
		s_replicationTaskMaxTimeMillis = serverConfig.getInt(ServerConstants.SCHEMA_REPLICATION_MAX_TASK_TIME_SECS) * 1000;
		s_schemaMaxTaskAttemptCount = serverConfig.getInt(ServerConstants.RESOURCE_MAXIMUM_TASK_ATTEMPT_COUNT);
		s_replicationIntervalMillis = serverConfig.getInt(ServerConstants.SCHEMA_REPLICATIONTHREAD_INTERVAL_SECS) * 1000;
	}

	public SchemaInstance(String vdbNameUpper, URL masterURL, URL serviceURL, List<String> rules) throws RGMAPermanentException {
		m_vdbNameUpper = vdbNameUpper;
		m_masterURL = masterURL;
		m_serviceURL = serviceURL;
		m_schemaDatabase = SchemaDatabaseFactory.getSchemaDatabase(vdbNameUpper, masterURL, serviceURL, rules);
		m_replicationStatus = new ReplicationStatus();
		/* The first update is scheduled to run immediately */
		if (!m_masterURL.equals(m_serviceURL)) {
			m_Timer = new Timer(true);
			m_Timer.schedule(new ReplicateTimerTask(), 0, s_replicationIntervalMillis);
		}
	}

	boolean alter(String torv, String tableName, String action, String name, String type, UserContextInterface requestContext) throws RGMAPermanentException {
		return m_schemaDatabase.alter(torv, tableName, action, name, type, requestContext);
	}

	boolean createIndex(String createIndexStatement, UserContextInterface requestContext) throws RGMAPermanentException {
		return m_schemaDatabase.createIndex(createIndexStatement, requestContext);
	}

	boolean createTable(String createStatement, List<String> tableAuthz, UserContextInterface requestContext) throws RGMAPermanentException {
		return m_schemaDatabase.createTable(createStatement, tableAuthz, requestContext);
	}

	boolean createView(String createViewStatement, List<String> tableAuthz, UserContextInterface requestContext) throws RGMAPermanentException {
		return m_schemaDatabase.createView(createViewStatement, tableAuthz, requestContext);
	}

	boolean dropIndex(String tableName, String indexName, UserContextInterface requestContext) throws RGMAPermanentException {
		return m_schemaDatabase.dropIndex(tableName, indexName, requestContext);
	}

	boolean dropTable(String tableName, UserContextInterface requestContext) throws RGMAPermanentException {
		return m_schemaDatabase.dropTable(tableName, requestContext);
	}

	boolean dropView(String viewName, UserContextInterface requestContext) throws RGMAPermanentException {
		return m_schemaDatabase.dropView(viewName, requestContext);
	}

	List<TupleSet> getAllSchema() throws RGMAPermanentException {
		return m_schemaDatabase.getAllSchema();
	}

	List<String> getAllTables(UserContextInterface userContext) throws RGMAPermanentException {
		return m_schemaDatabase.getAllTables(userContext);
	}

	synchronized void getAndPutReplica() throws RGMAPermanentException, RemoteException, RGMATemporaryException {
		long masterTime = m_schemaDatabase.getMasterTime();
		List<TupleSet> records = RemoteSchema.getSchemaUpdates(m_masterURL, m_vdbNameUpper, masterTime);
		LOG.debug("getAndPutReplica obtained " + records.size() + " ResultSets with getSchemaUpdates since " + masterTime + " for " + m_vdbNameUpper + " from "
				+ m_masterURL);
		try {
			m_schemaDatabase.putSchemaUpdates(records);
		} catch (RGMAPermanentException e) {
			LOG.warn("getAndPutReplica got internal exception from putSchemaUpdates for " + m_vdbNameUpper + " " + e.getFlattenedMessage());
			records = RemoteSchema.getAllSchema(m_masterURL, m_vdbNameUpper);
			LOG.debug("getAndPutReplica returned " + records.size() + " ResultSets with getAllSchema for " + m_vdbNameUpper);
			m_schemaDatabase.putAllSchema(records);
		}
	}

	List<String> getAuthorizationRules(String tableName, UserContextInterface userContext) throws RGMAPermanentException {
		return m_schemaDatabase.getAuthorizationRules(tableName, userContext);
	}

	String getLastReplicationInfo() {
		if (m_serviceURL.equals(m_masterURL))
			return "This is the master";

		synchronized (m_replicationStatus) {
			StringBuffer s = new StringBuffer();
			if (m_replicationStatus.m_lastSuccessTime == 0) {
				s.append("No succesful replication yet");
			} else {
				s.append("Last successful replication was " + (System.currentTimeMillis() - m_replicationStatus.m_lastSuccessTime) / 1000 + " seconds ago.");
			}
			if (m_replicationStatus.m_lastErrorTime != 0) {
				s.append(" Error: " + m_replicationStatus.m_lastError + " occured " + (System.currentTimeMillis() - m_replicationStatus.m_lastErrorTime) / 1000
						+ " seconds ago.");
			}
			return s.toString();
		}
	}

	long getMasterTimestamp() throws RGMAPermanentException {
		return m_schemaDatabase.getMasterTime();
	}

	List<TupleSet> getSchemaUpdates(long timestamp) throws RGMAPermanentException {
		return m_schemaDatabase.getSchemaUpdates(timestamp);
	}

	SchemaTableDefinition getTableDefinition(String tableName, UserContextInterface userContext) throws RGMAPermanentException {
		return m_schemaDatabase.getTableDefinition(tableName, userContext);
	}

	List<SchemaIndex> getTableIndexes(String tableName, UserContextInterface userContext) throws RGMAPermanentException {
		return m_schemaDatabase.getTableIndexes(tableName, userContext);
	}

	long getTableTimestamp(String tableName) throws RGMAPermanentException {
		return m_schemaDatabase.getTableTimestamp(tableName);
	}

	void putAllSchema(List<TupleSet> fullSchema) throws RGMAPermanentException {
		m_schemaDatabase.putAllSchema(fullSchema);
	}

	void putSchemaUpdates(List<TupleSet> fullSchema) throws RGMAPermanentException {
		m_schemaDatabase.putSchemaUpdates(fullSchema);
	}

	boolean setAuthorizationRules(String tableName, List<String> authzRules, UserContextInterface requestContext) throws RGMAPermanentException {
		return m_schemaDatabase.setAuthorizationRules(tableName, authzRules, requestContext);
	}

	void setMaster(URL masterURL) throws RGMAPermanentException {
		boolean iAmTheMaster = masterURL.equals(m_serviceURL);
		if (m_Timer != null && iAmTheMaster) {
			m_Timer.cancel();
		} else if (m_Timer == null && !iAmTheMaster) {
			m_Timer = new Timer(true);
			m_Timer.schedule(new ReplicateTimerTask(), s_replicationIntervalMillis, s_replicationIntervalMillis);
		}
		m_masterURL = masterURL;
	}

	void setSchemaRules(List<String> rules) {
		m_schemaDatabase.setSchemaRules(rules);
	}

	void shutdown() {
		if (m_Timer != null) {
			m_Timer.cancel();
		}
		if (m_schemaDatabase != null) {
			m_schemaDatabase.shutdown();
		}
	}
}
