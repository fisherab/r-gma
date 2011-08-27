/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.schema;

import java.net.URL;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteSchema;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.VDBConfigurator;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.SchemaIndex;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserContextInterface;

/**
 * Schema Service
 */
public class SchemaService extends Service {
	private class Vdb {
		SchemaInstance m_instance;
		URL m_masterURL;
		String m_vdbName;

		Vdb(String vdbName, SchemaInstance instance, URL master) {
			m_vdbName = vdbName;
			m_instance = instance;
			m_masterURL = master;
		}
	}

	private final static Logger LOG = Logger.getLogger(SchemaConstants.SCHEMA_LOGGER);

	private static final Object s_instanceLock = new Object();

	private static SchemaService s_schemaService;

	private static TaskManager s_taskManager;

	private int m_replicationInterValSecs;

	/** The URL of this service */
	private URL m_serviceURL;

	/** Mapping from vdbNameUpper to Vdb */
	private final Map<String, Vdb> m_vdbs = new HashMap<String, Vdb>();

	public static void dropInstance() {
		synchronized (s_instanceLock) {
			if (s_schemaService != null) {
				s_schemaService.shutdown();
				s_schemaService = null;
			}
		}
	}

	public static SchemaService getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_schemaService == null) {
				s_schemaService = new SchemaService();
			}
			return s_schemaService;
		}
	}

	/**
	 * Creates a new SchemaService object.
	 */
	private SchemaService() throws RGMAPermanentException {
		super(ServerConstants.SCHEMA_SERVICE_NAME);
		SchemaInstance.setStaticVariables();
		try {
			m_serviceURL = getURL();
			VDBConfigurator.getInstance().registerSchemaService(this);
			m_replicationInterValSecs = ServerConfig.getInstance().getInt(ServerConstants.SCHEMA_REPLICATIONTHREAD_INTERVAL_SECS);
		} catch (RGMAPermanentException e) {
			s_controlLogger.fatal("Error occured during schema startup: " + e.getMessage());
			throw e;
		}
		s_controlLogger.info("SchemaService started");
		s_taskManager = TaskManager.getInstance();
	}

	public boolean alter(String vdbName, boolean canForward, String torv, String tableName, String action, String name, String type, UserContext userContext)
			throws RGMATemporaryException, RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		boolean result = false;
		try {
			Vdb vdb = getVdb(vdbName);
			if (vdb.m_masterURL.equals(m_serviceURL)) {
				result = vdb.m_instance.alter(torv, tableName, action, name, type, userContext);
			} else if (RemoteSchema.alter(vdb.m_masterURL, vdbName, torv, tableName, action, name, type, userContext)) {
				vdb.m_instance.getAndPutReplica();
				result = true;
			}
		} catch (RemoteException e) {
			LOG.info("Alter in VDB " + vdbName + " failed due to network connection error to master schema service");
			throw new RGMATemporaryException("Alter in VDB " + vdbName + " failed due to network connection error to master schema service");
		} catch (RGMAPermanentException e) {
			LOG.info("alter " + action + " " + name + (type == null ? "" : " " + type) + " in VDB " + vdbName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Executed: " + "alter " + action + " " + name + (type == null ? "" : " " + type));
		}
		return result;
	}

	/**
	 * Adds a new index definition for an existing table in a schema of the requested virtual database.
	 */
	public boolean createIndex(String vdbName, boolean canForward, String createIndexStatement, UserContextInterface requestContext)
			throws RGMAPermanentException, RGMATemporaryException, RGMAPermanentException, RGMAPermanentException {
		boolean result = false;
		try {
			Vdb vdb = getVdb(vdbName);
			if (vdb.m_masterURL.equals(m_serviceURL)) {
				result = vdb.m_instance.createIndex(createIndexStatement, requestContext);
			} else if (RemoteSchema.createIndex(vdb.m_masterURL, vdbName, createIndexStatement, requestContext)) {
				vdb.m_instance.getAndPutReplica();
				result = true;
			}
		} catch (RemoteException e) {
			LOG.info(createIndexStatement + " in VDB " + vdbName + " failed due to network connection error to master schema service");
			throw new RGMATemporaryException(createIndexStatement + " in VDB " + vdbName + " failed due to network connection error to master schema service");
		} catch (RGMAPermanentException e) {
			LOG.info(createIndexStatement + " in VDB " + vdbName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Executed: " + createIndexStatement);
		}
		return result;
	}

	public boolean createTable(String vdbName, boolean canForward, String createTableStatement, List<String> tableAuthz, UserContextInterface requestContext)
			throws RGMATemporaryException, RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		boolean result = false;
		try {
			Vdb vdb = getVdb(vdbName);
			if (vdb.m_masterURL.equals(m_serviceURL)) {
				result = vdb.m_instance.createTable(createTableStatement, tableAuthz, requestContext);
			} else if (RemoteSchema.createTable(vdb.m_masterURL, vdbName, createTableStatement, tableAuthz, requestContext)) {
				vdb.m_instance.getAndPutReplica();
				result = true;
			}
		} catch (RGMAPermanentException e) {
			LOG.info("Create table " + createTableStatement + " in VDB " + vdbName + " failed: " + e.getFlattenedMessage());
			throw e;
		} catch (RemoteException e) {
			LOG.info(createTableStatement + " in VDB " + vdbName + " failed due to network connection error to master schema service");
			throw new RGMATemporaryException(createTableStatement + " in VDB " + vdbName + " failed due to network connection error to master schema service");
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Executed: " + createTableStatement);
		}
		return result;
	}

	/**
	 * This dynamically sets up a new VDB. If the VDB already exists its display name should be updated if necessary to
	 * match the case and the master schema and rules updated as necessary. It is possible that a request is received
	 * when there is nothing to do.
	 * 
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 * @throws DatabaseException
	 * @throws RGMAPermanentException
	 */
	public void createVDB(String vdbName, URL master, List<String> rules) throws RGMAPermanentException {
		String vdbNameUpper = vdbName.toUpperCase();
		try {
			Vdb vdb = m_vdbs.get(vdbNameUpper);
			if (vdb == null) {
				SchemaInstance instance = new SchemaInstance(vdbNameUpper, master, m_serviceURL, rules);
				Timestamp now = new Timestamp(System.currentTimeMillis());
				long masterTimestamp = instance.getMasterTimestamp();
				if (now.getTime() < masterTimestamp) {
					instance.shutdown();
					throw new RGMAPermanentException("System Clock Error: Current time " + now.toString() + " is in the past");
				}
				vdb = new Vdb(vdbName, instance, master);
				m_vdbs.put(vdbNameUpper, vdb);
			} else {
				vdb.m_vdbName = vdbName;
				if (!vdb.m_masterURL.equals(master)) {
					vdb.m_instance.setMaster(master);
				}
				vdb.m_instance.setSchemaRules(rules);
			}
		} catch (RGMAPermanentException e) {
			throw new RGMAPermanentException("Problem setting up schema instance " + vdbName, e);
		}

		StringBuilder s = new StringBuilder("Created or updated VDB ");
		s.append(vdbName).append(" with master ").append(master).append(" and rules: ");
		for (String r : rules) {
			s.append('"').append(r).append('"').append(' ');
		}
		s_controlLogger.info(s);
	}

	/**
	 * Adds a new view definition on an existing table in a schema of the requested virtual database.
	 */
	public boolean createView(String vdbName, boolean canForward, String createViewStatement, List<String> viewAuthz, UserContextInterface requestContext)
			throws RGMAPermanentException, RGMAPermanentException, RGMATemporaryException, RGMAPermanentException {
		boolean result = false;
		try {
			Vdb vdb = getVdb(vdbName);
			if (vdb.m_masterURL.equals(m_serviceURL)) {
				result = vdb.m_instance.createView(createViewStatement, viewAuthz, requestContext);
			} else if (RemoteSchema.createView(vdb.m_masterURL, vdbName, createViewStatement, viewAuthz, requestContext)) {
				vdb.m_instance.getAndPutReplica();
				result = true;
			}
		} catch (RemoteException e) {
			LOG.info("Create view in VDB " + vdbName + " failed due to network connection error to master schema service");
			throw new RGMATemporaryException("Create view in VDB " + vdbName + " failed due to network connection error to master schema service");
		} catch (RGMAPermanentException e) {
			LOG.info(createViewStatement + " for " + vdbName + " VDB failed. " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			LOG.info(createViewStatement + " for " + vdbName + " VDB failed. " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Executed: " + createViewStatement);
		}
		return result;
	}

	/**
	 * This dynamically disables a VDB. The schema on disk is left to be re-used. This is to cope with the case when a
	 * VDB is disabled and then immediately created again.
	 */
	public void disableVDB(String vdbName) {
		String vdbNameUpper = vdbName.toUpperCase();
		Vdb vdb = m_vdbs.get(vdbNameUpper);
		if (vdb != null) {
			vdb.m_instance.shutdown();
			m_vdbs.remove(vdbNameUpper);
			s_controlLogger.info("Disabled VDB " + vdbName);
		}
	}

	/**
	 * Drops an index from the schema of the requested virtual database.
	 */
	public boolean dropIndex(String vdbName, String tableName, boolean canForward, String indexName, UserContextInterface requestContext)
			throws RGMATemporaryException, RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		boolean result = false;
		try {
			checkUserInputValid(tableName);
			checkUserInputValid(indexName);
			Vdb vdb = getVdb(vdbName);
			if (vdb.m_masterURL.equals(m_serviceURL)) {
				result = vdb.m_instance.dropIndex(tableName, indexName, requestContext);
			} else if (RemoteSchema.dropIndex(vdb.m_masterURL, vdbName, tableName, indexName, requestContext)) {
				vdb.m_instance.getAndPutReplica();
				result = true;
			}
		} catch (RemoteException e) {
			LOG.info("Drop index in VDB " + vdbName + " failed due to network connection error to master schema service");
			throw new RGMATemporaryException("Drop index in VDB " + vdbName + " failed due to network connection error to master schema service");
		} catch (RGMAPermanentException e) {
			LOG.info("Drop Index " + indexName + " from " + vdbName + "." + tableName + " failed: " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			LOG.info("Drop Index " + indexName + " from " + vdbName + "." + tableName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Dropped Index " + indexName + " from " + vdbName + "." + tableName);
		}
		return result;
	}

	/**
	 * Drops a table from the schema of the requested virtual database.
	 */
	public boolean dropTable(String vdbName, boolean canForward, String tableName, UserContextInterface requestContext) throws RGMAPermanentException,
			RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		boolean result = false;
		try {
			checkUserInputValid(tableName);
			Vdb vdb = getVdb(vdbName);
			if (vdb.m_masterURL.equals(m_serviceURL)) {
				result = vdb.m_instance.dropTable(tableName, requestContext);
			} else if (RemoteSchema.dropTable(vdb.m_masterURL, vdbName, tableName, requestContext)) {
				vdb.m_instance.getAndPutReplica();
				result = true;
			}
		} catch (RemoteException e) {
			LOG.info("Drop table " + vdbName + " failed due to network connection error to master schema service");
			throw new RGMATemporaryException("Drop table in VDB " + vdbName + " failed due to network connection error to master schema service");
		} catch (RGMAPermanentException e) {
			LOG.info("Drop table " + vdbName + "." + tableName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Dropped table " + vdbName + "." + tableName);
		}
		return result;
	}

	/**
	 * Drops a view from the schema of the requested virtual database.
	 */
	public boolean dropView(String vdbName, boolean canForward, String viewName, UserContextInterface requestContext) throws RGMATemporaryException,
			RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		boolean result = false;
		try {
			checkUserInputValid(viewName);
			Vdb vdb = getVdb(vdbName);
			if (vdb.m_masterURL.equals(m_serviceURL)) {
				result = vdb.m_instance.dropView(viewName, requestContext);
			} else if (RemoteSchema.dropView(vdb.m_masterURL, vdbName, viewName, requestContext)) {
				vdb.m_instance.getAndPutReplica();
				result = true;
			}
		} catch (RemoteException e) {
			LOG.info("Drop view in VDB " + vdbName + " failed due to network connection error to master schema service");
			throw new RGMATemporaryException("Drop view in VDB " + vdbName + " failed due to network connection error to master schema service");
		} catch (RGMAPermanentException e) {
			LOG.info("Drop a view failed: in " + vdbName + " VDB " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Successfully dropped view: viewName " + vdbName + "." + viewName);
		}
		return result;
	}

	public List<TupleSet> getAllSchema(String vdbName) throws RGMAPermanentException {
		List<TupleSet> records = null;
		try {
			Vdb vdb = m_vdbs.get(vdbName);
			if (vdb == null) {
				throw new RGMAPermanentException("VDB: " + vdbName + " is not recognised by this server");
			}
			records = vdb.m_instance.getAllSchema();
		} catch (RGMAPermanentException e) {
			LOG.error("getAllSchema from " + vdbName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Success getAllSchema " + vdbName + " returned " + records.size() + " records.");
		}
		return records;
	}

	/**
	 * Returns a list of all table names in the schema of the requested virtual database.
	 * 
	 * @param userContext
	 */
	public List<String> getAllTables(String vdbName, UserContextInterface userContext) throws RGMAPermanentException {
		List<String> alltables = null;
		try {
			Vdb vdb = getVdb(vdbName);
			alltables = vdb.m_instance.getAllTables(userContext);
		} catch (RGMAPermanentException e) {
			LOG.info("getAllTables fron VDB: " + vdbName + "failed :" + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Success getAllTables from " + vdbName + " returned " + alltables.size() + " results.");
		}
		return alltables;
	}

	/**
	 * Return a list of authorization rules
	 * 
	 * @param userContext
	 */
	public List<String> getAuthorizationRules(String vdbName, String tableName, UserContextInterface userContext) throws RGMAPermanentException {
		List<String> authzRules = null;
		try {
			checkUserInputValid(tableName);
			Vdb vdb = getVdb(vdbName);
			authzRules = vdb.m_instance.getAuthorizationRules(tableName, userContext);
		} catch (RGMAPermanentException e) {
			LOG.info("getAuthorizationRules from table: " + vdbName + "." + tableName + " failed: " + e.getMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Executed getAuthorizationRules from table: " + vdbName + "." + tableName + " returned " + authzRules.size() + " rules.");
		}
		return authzRules;
	}

	public String getProperty(String name, String param) throws RGMAPermanentException {
		String result = null;
		try {
			try {
				result = getServiceProperty(name, param);
			} catch (RGMAPermanentException e) {
				result = getProp(name, param);
			}
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to get property: " + name + ":" + param + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("GetProperty: " + name + ":" + param + " executed");
		}
		return result;
	}

	public List<TupleSet> getSchemaUpdates(String vdbName, long ts) throws RGMAPermanentException {
		List<TupleSet> records = null;
		try {
			Vdb vdb = m_vdbs.get(vdbName);
			if (vdb == null) {
				throw new RGMAPermanentException("VDB: " + vdbName + " is not recognised by this server");
			}
			records = vdb.m_instance.getSchemaUpdates(ts);
		} catch (RGMAPermanentException e) {
			LOG.error("getSchemaUpdates from " + vdbName + " failed " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("getSchemaUpdates from " + vdbName + " returned " + records.size() + " ResultSets");
		}
		return records;
	}

	/**
	 * Returns a table's column definitions from the schema of the requested virtual database (used by the R-GMA
	 * Browser).
	 * 
	 * @param userContext
	 */
	public SchemaTableDefinition getTableDefinition(String vdbName, String tableName, UserContext userContext) throws RGMAPermanentException,
			RGMAPermanentException {
		SchemaTableDefinition tableDef = null;
		try {
			checkUserInputValid(tableName);
			Vdb vdb = getVdb(vdbName);
			tableDef = vdb.m_instance.getTableDefinition(tableName, userContext);
		} catch (RGMAPermanentException e) {
			LOG.info("getTableDefinition for table: " + vdbName + "." + tableName + "failed. " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Got table definition for " + vdbName + "." + tableName);
		}
		return tableDef;
	}

	/**
	 * Return a list of indices on the specified table
	 * 
	 * @param userContext
	 */
	public List<SchemaIndex> getTableIndexes(String vdbName, String tableName, UserContext userContext) throws RGMAPermanentException, RGMAPermanentException {
		List<SchemaIndex> tableIndexes = null;
		try {
			checkUserInputValid(tableName);
			Vdb vdb = getVdb(vdbName);
			tableIndexes = vdb.m_instance.getTableIndexes(tableName, userContext);
		} catch (RGMAPermanentException e) {
			LOG.info("getTableIndexes from table " + vdbName + "." + tableName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("returned " + tableIndexes.size() + " getTableIndexes from table " + vdbName + "." + tableName);
		}
		return tableIndexes;
	}

	public long getTableTimestamp(String vdbName, String tableName) throws RGMAPermanentException, RGMAPermanentException {
		long tableTS = 0;
		try {
			checkUserInputValid(tableName);
			Vdb vdb = getVdb(vdbName);
			tableTS = vdb.m_instance.getTableTimestamp(tableName);
		} catch (RGMAPermanentException e) {
			LOG.info("getTableTimestamp for " + vdbName + "." + tableName + " failed");
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("returned " + tableTS + " getTableTimestamp for " + vdbName + "." + tableName);
		}
		return tableTS;
	}

	public void ping(String vdbName) throws RGMAPermanentException {
		checkOnline();
		if (LOG.isInfoEnabled()) {
			LOG.info("Exiting SchemaService::ping");
		}
	}

	public void putAllSchema(String vdbName, List<TupleSet> records) throws RGMAPermanentException {
		try {
			Vdb vdb = getVdb(vdbName);
			vdb.m_instance.putAllSchema(records);
		} catch (RGMAPermanentException e) {
			LOG.error("putAllSchema to: " + vdbName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Put " + records.size() + " records in putAllSchema for vdb " + vdbName);
		}
	}

	public void putSchemaUpdates(String vdbName, List<TupleSet> records) throws RGMAPermanentException {
		try {
			Vdb vdb = getVdb(vdbName);
			vdb.m_instance.putSchemaUpdates(records);
		} catch (RGMAPermanentException e) {
			LOG.error("putSchemaUpdates to: " + vdbName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Put " + records.size() + " records in putSchemaUpdates for vdb " + vdbName);
		}
	}

	/**
	 * Set authorization rules
	 */
	public boolean setAuthorizationRules(String vdbName, boolean canForward, String tableName, List<String> rules, UserContextInterface requestContext)
			throws RGMATemporaryException, RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		boolean result = false;
		try {
			checkUserInputValid(tableName);
			Vdb vdb = getVdb(vdbName);
			if (vdb.m_masterURL.equals(m_serviceURL)) {
				result = vdb.m_instance.setAuthorizationRules(tableName, rules, requestContext);
			} else if (RemoteSchema.setAuthorizationRules(vdb.m_masterURL, vdbName, tableName, rules, requestContext)) {
				vdb.m_instance.getAndPutReplica();
				result = true;
			}
		} catch (RemoteException e) {
			LOG.info("SetAuthorizationRules in VDB " + vdbName + " failed due to network connection error to master schema service");
			throw new RGMATemporaryException("SetAuthorizationRules in VDB " + vdbName + " failed due to network connection error to master schema service");
		} catch (RGMAPermanentException e) {
			LOG.info("setAuthorizationRules for table " + vdbName + "." + tableName + " failed: " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			LOG.info("setAuthorizationRules for table " + vdbName + "." + tableName + " failed: " + e.getFlattenedMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Set " + rules.size() + " rules in setAuthorizationRules for table " + vdbName + "." + tableName);
		}
		return result;
	}

	private String getLastReplicationInfo(String vdbNameUpper) {
		Vdb vdb = m_vdbs.get(vdbNameUpper);
		return vdb.m_instance.getLastReplicationInfo();
	}

	/**
	 * Returns properties specific to the schema service
	 */
	private String getProp(String name, String param) throws RGMAPermanentException {
		String result = null;
		if (name.equalsIgnoreCase(ServerConstants.SERVICE_RESOURCES)) {
			StringBuilder b = new StringBuilder();
			b.append("<Schema ");
			b.append("ReplicationIntervalMillis=\"" + Integer.toString(m_replicationInterValSecs * 1000) + "\">\n");
			for (String vdbNameUpper : m_vdbs.keySet()) {
				b.append(getVDBDetails(vdbNameUpper, false));
			}
			b.append("</Schema>\n");
			result = wrapData(b.toString());
			result = b.toString();

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_STATUS)) {
			getVdb(param);
			StringBuilder b = new StringBuilder();
			b.append("<Schema>\n");
			b.append(getVDBDetails(param, true));
			b.append("</Schema>\n");
			result = wrapData(b.toString());
			result = b.toString();

		} else {
			throw new RGMAPermanentException("Unrecognised property name: " + name);
		}
		return result;
	}

	/**
	 * @return A string representation of the tasks belonging to this VDB resource (in XML).
	 */
	private String getTasks(String vdbNameUpper) {
		StringBuilder b = new StringBuilder();
		List<Map<String, String>> tasks = s_taskManager.getTasks(m_serviceURL.toString() + vdbNameUpper);
		for (Map<String, String> task : tasks) {
			b.append("<Task ");
			for (String key : task.keySet()) {
				b.append(key).append("=\"").append(task.get(key)).append("\" ");
			}
			b.append("/>\n");
		}
		return b.toString();
	}

	private Vdb getVdb(String vdbName) throws RGMAPermanentException {
		String vdbNameUpper = vdbName.toUpperCase();
		Vdb vdb = m_vdbs.get(vdbNameUpper);
		if (vdb == null) {
			throw new RGMAPermanentException("VDB: " + vdbName + " is not recognised by this server");
		}
		return vdb;
	}

	/**
	 * @return A detailed summary of this particular VDB.
	 */
	private String getVDBDetails(String vdbName, boolean fullDetails) {
		String vdbNameUpper = vdbName.toUpperCase();
		Vdb vdb = m_vdbs.get(vdbNameUpper);
		StringBuilder b = new StringBuilder();
		b.append("<VDB ID=\"");
		b.append(vdb.m_vdbName);
		b.append("\" MasterURL=\"");
		b.append(vdb.m_masterURL);
		b.append("\" LastSuccessfulReplicationIntervalMillis=\"");
		b.append(getLastReplicationInfo(vdbNameUpper));
		b.append("\">\n");
		if (fullDetails) {
			b.append(getTasks(vdbNameUpper));
		}
		b.append("</VDB>\n");
		return b.toString();
	}

	/**
	 * shutdown this service
	 */
	private void shutdown() {
		setOffline("Shutting down");
		for (Vdb vdb : m_vdbs.values()) {
			vdb.m_instance.shutdown();
		}
		s_controlLogger.info("SchemaService shutdown");
	}
}