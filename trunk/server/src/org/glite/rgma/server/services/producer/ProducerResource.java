package org.glite.rgma.server.services.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteConsumable;
import org.glite.rgma.server.remote.RemoteResourceBase;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.producer.ondemand.OnDemandCursor;
import org.glite.rgma.server.services.producer.ondemand.OnDemandProducerResource;
import org.glite.rgma.server.services.producer.store.QueryTypeNotSupportedException;
import org.glite.rgma.server.services.producer.store.TupleCursor;
import org.glite.rgma.server.services.producer.store.TupleStore;
import org.glite.rgma.server.services.producer.store.TupleStoreManager;
import org.glite.rgma.server.services.registry.RegistryService;
import org.glite.rgma.server.services.resource.Resource;
import org.glite.rgma.server.services.schema.SchemaService;
import org.glite.rgma.server.services.sql.CreateTableStatement;
import org.glite.rgma.server.services.sql.ProducerPredicate;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.TableName;
import org.glite.rgma.server.services.sql.TableReference;
import org.glite.rgma.server.services.sql.Validator;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.streaming.StreamingSender;
import org.glite.rgma.server.services.tasks.Task;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.system.ConsumerEntry;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.ProducerProperties;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.Storage;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.Units;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserSystemContext;
import org.glite.rgma.server.system.Storage.StorageType;

public abstract class ProducerResource extends Resource {

	protected static final Logger s_securityLogger = Logger.getLogger(ServerConstants.SECURITY_LOGGER);

	protected static String s_hostname;

	/** Mediator component used to create and maintain query plans */
	protected static RegistryService s_registry;

	/** Connection to a schema service. */
	protected static SchemaService s_schema;

	/** Number of tuples in a streamed result set */
	protected static int s_streamingChunkSize;

	/** Queue for asynchronous messaging */
	protected static TaskManager s_taskInvocationQueue;

	private static TupleStoreManager s_DBTupleStoreManager;

	private static int s_maximumTaskAttemptCount;

	private static long s_maximumTaskTimeMillis;

	private static TupleStoreManager s_MEMTupleStoreManager;

	private static int s_registryTerminationIntervalSecs;

	private static int s_schemaTableUpdateInSec;

	protected static StreamingSender s_streamingSender;

	private Long m_timeToGiveUpOnUnreachable  = 0L;

	/** When a remote exception will be treated as an unknown resource exception */
	private static long s_intervalToGiveUpOnUnreachableMillis;

	/**
	 * This method is called once when the Primary or Secondary Producer Service starts up to // * initialize static
	 * variables
	 * 
	 * @throws ConfigurationException
	 * @throws RGMAPermanentException
	 */
	protected static void setStaticVariables(TimeInterval registryTerminationInterval, String hostname) throws RGMAPermanentException {
		Resource.setStaticVariables();
		s_registry = RegistryService.getInstance();
		s_schema = SchemaService.getInstance();
		s_taskInvocationQueue = TaskManager.getInstance();
		ServerConfig config = ServerConfig.getInstance();
		s_streamingChunkSize = config.getInt(ServerConstants.PRODUCER_MAX_TUPLE_COUNT_PER_STREAMED_CHUNK);
		s_maximumTaskTimeMillis = config.getInt(ServerConstants.CONSUMER_MAXIMUM_TASK_TIME_SECS) * 1000;
		s_maximumTaskAttemptCount = config.getInt(ServerConstants.RESOURCE_MAXIMUM_TASK_ATTEMPT_COUNT);
		/* TODO This consumer parameter is shared - ugh */
		s_schemaTableUpdateInSec = config.getInt(ServerConstants.CONSUMER_SCHEMA_CHECK_INTERVAL_SECS);
		s_streamingSender = StreamingSender.getInstance();
		s_registryTerminationIntervalSecs = (int) registryTerminationInterval.getValueAs(Units.SECONDS);
		s_hostname = hostname;
		s_DBTupleStoreManager = TupleStoreManager.getInstance(StorageType.DB);
		s_MEMTupleStoreManager = TupleStoreManager.getInstance(StorageType.MEM);
		s_intervalToGiveUpOnUnreachableMillis = config.getLong(ServerConstants.RESOURCE_INTERVAL_TO_GIVE_UP_ON_UNREACHABLE_SECS) * 1000;
	}

	/** Map from Table(with VDB) to table */
	public final Map<String, Table> m_tables;

	/**
	 * Holds the last inserted time for tuple
	 */
	protected long lastinsertedTime;

	/** Holds the last time the registry is updated */
	protected long m_lastRegistryUpdate;

	/** Holds the ProducerProperties values* */
	protected ProducerProperties m_properties = null;

	/** Thread responsible for timing out the query and periodically checking status of consumer */
	protected Timer m_Timer;

	/** TupleStore instance */
	protected TupleStore m_tupleStore;

	protected TupleStoreManager m_tupleStoreManager;

	/**
	 * Holds the count on total inserted tuples
	 */
	protected int totalInsertedTuples;

	public ProducerResource(ResourceEndpoint endpoint, UserContext userContext, ProducerProperties properties, Logger logger) throws RGMAPermanentException,
			RGMAPermanentException {
		super(endpoint, userContext, logger);
		m_properties = properties;
		m_tables = new HashMap<String, Table>();
		m_Timer = new Timer(true);
		m_Timer.schedule(new SchemaUpdatedTask(), s_schemaTableUpdateInSec * 1000, s_schemaTableUpdateInSec * 1000);
		Storage st = m_properties.getStorage();
		if (st != null) {
			String logicalName = st.getLogicalName();
			m_tupleStoreManager = st.isMemory() ? s_MEMTupleStoreManager : s_DBTupleStoreManager;
			m_tupleStore = m_tupleStoreManager.createTupleStore(m_context.getDN(), logicalName, m_properties.isLatest(), endpoint);
			m_logger.debug("Created TS " + m_tupleStore + " for " + m_properties);
		}
	}

	/**
	 * Abort the currently running query for given consumer
	 * 
	 * @throws RGMAPermanentException
	 */
	public void abortQuery(ResourceEndpoint consumerEp) throws RGMAPermanentException {
		synchronized (m_tables) {
			for (Table t : m_tables.values()) {
				synchronized (t.m_queries) {
					Query q = t.m_queries.remove(consumerEp);
					if (q != null) {
						q.m_runningQuery.abort();
					}
				}
				synchronized (t.m_continuousConsumers) {
					t.m_continuousConsumers.remove(consumerEp);
				}
			}
		}
	}

	/**
	 * Gets the HistoryRetentionPeriod.
	 * 
	 * @return HistoryRetentionPeriod The minimum time for which history tuples are stored.
	 * @throws RGMAException
	 *             Thrown if not connected.
	 */
	public int getHistoryRetentionPeriod(String tableName) throws RGMAPermanentException {
		TableName ctn = new TableName(tableName);
		if (ctn.getVdbName() == null) {
			throw new RGMAPermanentException("Table name must include the vdb prefix");
		}
		String vdbTableName = ctn.getVdbTableName();
		synchronized (m_tables) {
			Table t = m_tables.get(vdbTableName);
			if (t == null) {
				throw new RGMAPermanentException("Table not declared: " + tableName);
			} else {
				return t.m_hrpSecs;
			}
		}
	}

	/**
	 * Returns the producer properties
	 */
	protected ProducerProperties getProducerProperties() {
		return m_properties;
	}

	/**
	 * Start streaming tuples for the given consumer
	 * 
	 * @throws NumericException
	 *             to indicate that the query was not good. This is only caused by unexpected restrictions in the RDBMS
	 *             implementation. The Start Task of the NormalReply will abort the query and save the exception to be
	 *             returned to the user on the next pop.
	 */
	synchronized public TupleSet start(String selectString, TimeInterval timeout, QueryProperties queryProps, ResourceEndpoint consumerEp,
			StreamingProperties streamingProps, UserSystemContext context) throws RGMAPermanentException, RGMAPermanentException, NumericException {

		if (timeout != null) {
			if (timeout.getValueAs(Units.MILLIS) < 0) {
				throw new RGMAPermanentException("Time interval may not be negative");
			}
		}

		TupleSet resultSet = new TupleSet();
		SelectStatement select = null;
		try {
			select = SelectStatement.parse(selectString);
			if (!select.isSimpleQuery()) {
				if (queryProps.isContinuous()) {
					throw new RGMAPermanentException("Complex continuous queries are not valid");
				}
				if (queryProps.isStatic()) {
					throw new RGMAPermanentException("Complex static queries are not valid");
				}
			}
			Map<String, SchemaTableDefinition> schemaDef = new HashMap<String, SchemaTableDefinition>();
			List<TableReference> fromTables = select.getFrom();
			Table table = null;
			Set<String> vdbNames = new HashSet<String>();
			String firstVdbTableName = null;
			synchronized (m_tables) {
				for (TableReference reference : fromTables) {
					String vdbTableName = reference.getTable().getVdbTableName();
					if (firstVdbTableName == null) {
						firstVdbTableName = vdbTableName;
					}

					table = m_tables.get(vdbTableName);
					if (table == null) {
						throw new RGMAPermanentException("Start called for table " + vdbTableName + " not known to producer");
					}

					/* Populate the set of vdbNames so that we can be sure the requesting machine is allowed to ask */
					vdbNames.add(table.m_vdbName);

					/* fill schemaDef table so that the select can be validated */
					schemaDef.put(vdbTableName, table.m_def);
					resultSet.addRow(new String[] { vdbTableName, "" + table.m_tableUpdateTime });
				}

				for (String vdbName : vdbNames) {
					try {
						Service.checkSystemContext(vdbName, context);
					} catch (RGMAPermanentException e) {
						s_securityLogger.warn(e.getFlattenedMessage());
						throw new RGMAPermanentException("Serious security/configuration problem: " + e.getMessage());
					}
				}

				try {
					Validator v = new Validator(select, schemaDef);
					v.validate();
				} catch (RGMAPermanentException e) {
					throw new RGMAPermanentException(e);
				}
			}

			long startTimeMS = System.currentTimeMillis();
			if (queryProps.hasTimeInterval()) {
				startTimeMS = System.currentTimeMillis() - queryProps.getTimeInterval().getValueAs(Units.MILLIS);
			}
			RunningQuery runningQuery = null;
			/*
			 * Protect TupleStore and Timer from getting messages after this resource has been destroyed (and the tuple
			 * store closed and the timer cancelled).
			 */
			if (m_status != Status.DESTROYED) {
				TupleCursor cursor = null;
				if (m_tupleStore != null) {
					/*
					 * This next call can throw a Numeric Exception. This will be rare and is only in the case where the
					 * RDBMS has found a problem that the consumer did not spot.
					 */
					cursor = m_tupleStore.openCursor(select, queryProps, startTimeMS, context, consumerEp);
				} else {
					/*
					 * This is a bit ugly as it involves a cast - i.e. the ProducerResource is behaving specially for
					 * the OnDemandProducer
					 */
					cursor = new OnDemandCursor(select, (OnDemandProducerResource) this, context, consumerEp, timeout, table.m_vdbTableName);
				}
				int streamingChunkSize = Math.min(streamingProps.getChunkSize(), s_streamingChunkSize);
				runningQuery = new RunningQuery(streamingProps, cursor, m_tupleStore, streamingChunkSize, m_endpoint, consumerEp, selectString, queryProps
						.isContinuous(), s_streamingSender, firstVdbTableName);
				s_streamingSender.addQuery(runningQuery);
				if (timeout != null) {
					m_Timer.schedule(new TimeoutTask(consumerEp), timeout.getValueAs(Units.MILLIS));
				}
				Query query = new Query(runningQuery, streamingProps.getStreamingPort(), select.getWhere() != null, queryProps.isContinuous());
				synchronized (table.m_queries) {
					table.m_queries.put(consumerEp, query);
				}
			}

			/*
			 * Now that the query has been added to table.m_queries we can set
			 * table.m_continuousConsumers.get(consumerEp).m_connected to be true. This sequence ensures that the PP
			 * canDestroy() method works correctly.
			 */
			if (queryProps.isContinuous()) {
				synchronized (table.m_continuousConsumers) {
					ContinuousConsumer con = table.m_continuousConsumers.get(consumerEp);
					if (con == null) {
						con = new ContinuousConsumer(consumerEp);
						table.m_continuousConsumers.put(consumerEp, con);
					}
					con.m_connected = true;
				}
				if (m_logger.isDebugEnabled()) {
					m_logger.debug("Added a new continuous Consumer " + consumerEp + " to producer " + m_endpoint);
				}
			}

		} catch (ParseException e) {
			throw new RGMAPermanentException(e);
		} catch (QueryTypeNotSupportedException e) {
			throw new RGMAPermanentException(e);
		}
		return resultSet;

	}

	protected Table declareTable(UserContext userContext, TableName canonicalName, String predicate, int hrpSecs, boolean isSecondary)
			throws RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		if (hrpSecs < 0) {
			throw new RGMAPermanentException("HRP must be >= 0 and not " + hrpSecs);
		}
		predicate = predicate.trim();
		boolean isHistory = m_properties.isHistory();
		boolean isLatest = m_properties.isLatest();
		String tableName = canonicalName.getTableName();
		String vdbName = canonicalName.getVdbName();
		if (vdbName == null) {
			throw new RGMAPermanentException("Declared table must include the vdb prefix");
		}
		SchemaTableDefinition tableDef = s_schema.getTableDefinition(vdbName, tableName, null);
		List<String> authorizationList = s_schema.getAuthorizationRules(vdbName, tableName, null);
		CreateTableStatement createTableStmt = tableDef.getCreateTableStmt();
		ProducerPredicate producerPredicate;
		try {
			producerPredicate = ProducerPredicate.parse(predicate);
		} catch (ParseException e) {
			throw new RGMAPermanentException("Bad producer predicate '" + predicate + "': " + e.getMessage());
		}
		if (producerPredicate.isConsistent(createTableStmt) == false) {
			throw new RGMAPermanentException("Predicate not consistent with table definition");
		}
		if (m_tupleStore != null) {
			m_tupleStore.createTable(vdbName, createTableStmt, hrpSecs, authorizationList);
		}
		ProducerType producerType = new ProducerType(isHistory, isLatest, m_properties.isContinuous(), m_properties.isStatic(), isSecondary);

		return new Table(tableDef, hrpSecs, producerPredicate, s_schema.getTableTimestamp(vdbName, tableName), vdbName, tableName, producerType,
				authorizationList);
	}

	/**
	 * Returns the time in milliseconds since the registry was last updated or a message if never updated. The result is
	 * returned as a String.
	 */
	protected synchronized String getLastRegistryUpdateMillis() {
		if (m_lastRegistryUpdate == 0) {
			return "Not updated";
		} else {
			return String.valueOf(System.currentTimeMillis() - m_lastRegistryUpdate);
		}
	}

	/**
	 * Returns the Total No of Tuples in store
	 * 
	 * @return
	 * @throws RGMAPermanentException
	 */
	protected long getTotalNoTuplesInStore(String tableName) throws RGMAPermanentException {
		return m_tupleStore.getHistoryCount(tableName.toUpperCase());
	}

	protected void logReject(Table t, Exception e) {
		if (t != null) {
			synchronized (t) {
				t.m_lastRejectedTime = System.currentTimeMillis();
				t.m_totalRejectedTuples++;
				t.m_lastRejectedExceptionMsg = e.getMessage();
			}
		}
	}

	protected void registerTable(Table t, int hrt) throws RGMAPermanentException {
		synchronized (t) {
			if (t.m_registryTask != null) {
				t.m_registryTask.abort();
			}
			t.m_registryTask = new RegistrationTask(t, hrt);
			s_taskInvocationQueue.add(t.m_registryTask);
		}
	}

	private String generateUnRegistryKey() {
		StringBuilder registryKey = new StringBuilder("Registryfor:");
		Set<String> vdbList = new HashSet<String>();
		synchronized (m_tables) {
			for (Table t : m_tables.values()) {
				vdbList.add(t.m_vdbName);
			}
		}
		boolean hasNext = false;
		for (String vdbName : vdbList) {
			if (hasNext) {
				registryKey.append("&");
			} else {
				hasNext = true;
			}
			registryKey.append(vdbName);
		}
		return registryKey.toString();
	}

	/**
	 * TimerTask to abort the query when it reaches its timeout.
	 */
	public class TimeoutTask extends TimerTask {
		private ResourceEndpoint m_consumerEndpoint = null;

		public TimeoutTask(ResourceEndpoint consumerEndpoint) {
			m_consumerEndpoint = consumerEndpoint;
		}

		@Override
		public void run() {
			try {
				abortQuery(m_consumerEndpoint);
			} catch (RGMAPermanentException e) {
				m_logger.error("Error aborting query: " + e.getMessage());
			}
		}
	}

	public void destroy() throws RGMAPermanentException {
		try {
			synchronized (m_tables) {
				for (Table table : m_tables.values()) {
					synchronized (table.m_continuousConsumers) {
						for (Entry<ResourceEndpoint, ContinuousConsumer> e : table.m_continuousConsumers.entrySet()) {
							ResourceEndpoint consumerEp = e.getKey();
							ContinuousConsumer consumer = e.getValue();
							if (consumer.m_task != null) {
								consumer.m_task.abort();
							}
							s_taskInvocationQueue.add(new SendRemoveProducerTask(table, consumerEp));
							for (Table t : m_tables.values()) {
								synchronized (t.m_queries) {
									Query q = t.m_queries.get(consumerEp);
									if (q != null) {
										q.m_runningQuery.abort();
									}
								}
							}
						}
					}
				}
			}
			Task unregisterTask = new UnRegisterProducerTask();
			s_taskInvocationQueue.add(unregisterTask);
			if (m_tupleStoreManager != null) {
				m_tupleStoreManager.closeTupleStore(m_tupleStore);
			}
		} finally {
			m_Timer.cancel();
		}
	}

	/**
	 * Unregister producer table from the registry & aborts all the previous task(Registry,Consumer tasks). This is
	 * called by destroy of the primary and secondary producer so there is no need to tidy up data structures.
	 */
	public class UnRegisterProducerTask extends Task {

		public UnRegisterProducerTask() {
			super(m_endpoint.toString(), generateUnRegistryKey(), s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
		}

		@Override
		public Result invoke() {
			Result result = Result.SUCCESS;
			try {
				synchronized (m_tables) {
					for (Table table : m_tables.values()) {
						s_registry.unregisterProducerTable(table.m_vdbName, true, table.m_tableName, m_endpoint);
						if (m_logger.isInfoEnabled()) {
							m_logger.info("Unregistered " + table.m_vdbTableName + " for " + m_endpoint);
						}
					}
				}
			} catch (RGMAPermanentException e) {
				m_logger.error("Unexpected error while unregistering producer " + e.getFlattenedMessage());
				result = Result.HARD_ERROR;
			} catch (RGMATemporaryException e) {
				m_logger.warn("Failed to unregister Producer: " + e.getFlattenedMessage());
				result = Result.SOFT_ERROR;
			}
			return result;
		}
	}

	/** Holds information on continuous queries */
	protected class ContinuousConsumer {

		/** Set true when the start is received */
		public boolean m_connected;

		public final ResourceEndpoint m_consumerEp;

		public Task m_task;

		protected ContinuousConsumer(ResourceEndpoint consumerEp) {
			m_consumerEp = consumerEp;
		}
	}

	/** Holds information on a query of any kind */
	protected class Query {

		public final boolean m_hasPredicate;

		public final boolean m_isContinuous;

		public final RunningQuery m_runningQuery;

		public final int m_streamingPort;

		public Query(RunningQuery runningQuery, int streamingPort, boolean hasPredicate, boolean isContinuous) {
			m_runningQuery = runningQuery;
			m_streamingPort = streamingPort;
			m_hasPredicate = hasPredicate;
			m_isContinuous = isContinuous;
		}
	}

	/**
	 * Holds all material relating a table to a producer resource. Most attributes are final so no synchronization is
	 * required if they are to be examined. To access or set the statistics numbers synchronize on the Table object. The
	 * m_continuousConsumers and m_queries are both maps which are also marked final. To modify their contents
	 * synchronize on the collection object.
	 */
	public class Table {

		public final List<String> m_authz;

		public final Map<ResourceEndpoint, ContinuousConsumer> m_continuousConsumers;

		public final SchemaTableDefinition m_def;

		public final int m_hrpSecs;

		/** This field is changed */
		public long m_lastInsertTime;

		/** This field is changed */
		public long m_lastRegistryUpdate;

		/** This field is changed */
		public String m_lastRejectedExceptionMsg = "";

		/** This field is changed */
		public long m_lastRejectedTime;

		public final ProducerPredicate m_predicate;

		public final ProducerType m_producerType;

		public final Map<ResourceEndpoint, Query> m_queries;

		public RegistrationTask m_registryTask;

		public final long m_startTimeMillis;

		public final String m_tableName;

		public final long m_tableUpdateTime;

		/** This field is changed */
		public int m_totalInsertedTuples;

		/** This field is changed */
		public int m_totalRejectedTuples;

		public final String m_vdbName;

		public final String m_vdbTableName;

		public Table(SchemaTableDefinition def, int hrpSecs, ProducerPredicate producerPredicate, long tableUpdateTime, String vdbName, String tableName,
				ProducerType producerType, List<String> authz) {
			m_def = def;
			m_hrpSecs = hrpSecs;
			m_predicate = producerPredicate;
			m_tableUpdateTime = tableUpdateTime;
			m_vdbName = vdbName;
			m_tableName = tableName;
			m_producerType = producerType;
			m_authz = authz;
			m_continuousConsumers = new HashMap<ResourceEndpoint, ContinuousConsumer>();
			m_queries = new HashMap<ResourceEndpoint, Query>();
			m_startTimeMillis = System.currentTimeMillis();
			m_vdbTableName = vdbName + "." + tableName;
		}

		public Table(Table table) {
			m_def = table.m_def;
			m_hrpSecs = table.m_hrpSecs;
			m_predicate = table.m_predicate;
			m_tableUpdateTime = table.m_tableUpdateTime;
			m_vdbName = table.m_vdbName;
			m_tableName = table.m_tableName;
			m_producerType = table.m_producerType;
			m_authz = table.m_authz;
			m_continuousConsumers = table.m_continuousConsumers;
			m_queries = table.m_queries;
			m_startTimeMillis = table.m_startTimeMillis;
			m_vdbTableName = table.m_vdbTableName;

			/* Dynamic fields */
			m_lastInsertTime = table.m_lastInsertTime;
			m_lastRejectedExceptionMsg = table.m_lastRejectedExceptionMsg;
			m_lastRejectedTime = table.m_lastRejectedTime;
			m_totalInsertedTuples = table.m_totalInsertedTuples;
			m_totalRejectedTuples = table.m_totalRejectedTuples;
		}
	}

	/**
	 * Task which adds a consumer to the producer streaming list
	 */
	private class NotifyConsumerTask extends Task {

		private final ResourceEndpoint m_consumer;

		private final Table m_table;

		public NotifyConsumerTask(Table table, ResourceEndpoint consumer) {
			super(m_endpoint.toString(), consumer.getURL().toString(), s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_table = table;
			m_consumer = consumer;
		}

		@Override
		public Result invoke() {
			Result result = Result.SUCCESS;
			int hrpSecs = Math.min(m_table.m_hrpSecs, (int) ((System.currentTimeMillis() - m_table.m_startTimeMillis) / 1000L));
			ProducerTableEntry pte = new ProducerTableEntry(m_endpoint, m_table.m_vdbName, m_table.m_tableName, m_table.m_producerType, hrpSecs,
					m_table.m_predicate.toString());
			try {

				RemoteConsumable.addProducer(m_consumer.getURL(), m_consumer.getResourceID(), pte);
				if (m_logger.isDebugEnabled()) {
					m_logger.debug("Added producer: " + m_endpoint + " to consumer: " + m_consumer);
				}

			} catch (UnknownResourceException e) {
				m_logger.debug("Error sending add producer request to consumer: " + m_consumer + " from producer: " + m_endpoint + " " + e.getMessage());
				result = Result.SUCCESS;
			} catch (RemoteException e) {
				m_logger.debug("Error sending add producer request to consumer: " + m_consumer + " from producer: " + m_endpoint + " " + e.getMessage());
				result = Result.SOFT_ERROR;
			} catch (RGMAPermanentException e) {
				m_logger.debug("Error sending add producer request to consumer: " + m_consumer + " from producer: " + m_endpoint + " " + e.getMessage());
				result = Result.HARD_ERROR;
			} catch (RGMATemporaryException e) {
				m_logger.debug("Error sending add producer request to consumer: " + m_consumer + " from producer: " + m_endpoint + " " + e.getMessage());
				result = Result.SOFT_ERROR;
			}
			return result;
		}
	}

	/**
	 * Task which pings a consumable to see if it should be forgotten
	 */
	private class PingConsumableTask extends Task {

		private final ResourceEndpoint m_consumer;

		private final Table m_table;

		public PingConsumableTask(Table table, ResourceEndpoint consumer) {
			super(m_endpoint.toString(), consumer.getURL().toString(), s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_table = table;
			m_consumer = consumer;
		}

		@Override
		public Result invoke() {
			try {
				if (m_logger.isDebugEnabled()) {
					m_logger.debug("Sending ping to " + m_consumer);
				}
				RemoteResourceBase.ping(m_consumer.getURL(), m_consumer.getResourceID());
				synchronized (m_timeToGiveUpOnUnreachable) {
					m_timeToGiveUpOnUnreachable = 0L;
				}
				return Result.SUCCESS;
			} catch (UnknownResourceException e) {
				m_logger.info("Pinging " + m_consumer + " from " + m_endpoint + " gives unknown resource - so remove it");
				synchronized (m_table.m_continuousConsumers) {
					m_table.m_continuousConsumers.remove(m_consumer);
				}
				synchronized (m_table.m_queries) {
					Query q = m_table.m_queries.remove(m_consumer);
					if (q != null) {
						q.m_runningQuery.abort();
					}
				}
				return Result.SUCCESS;
			} catch (RemoteException e) {
				synchronized (m_timeToGiveUpOnUnreachable) {
					if (m_timeToGiveUpOnUnreachable == 0L) {
						m_timeToGiveUpOnUnreachable = System.currentTimeMillis() + s_intervalToGiveUpOnUnreachableMillis;
						m_logger.warn("RemoteException " + e.getMessage() + " sending ping request to " + m_consumer + " time noted to see if permanent");
						return Result.SOFT_ERROR;
					} else if (System.currentTimeMillis() > m_timeToGiveUpOnUnreachable) {
						m_logger.warn("RemoteException " + e.getMessage() + " sending ping request to " + m_consumer
								+ " appears to be permanent - remove it from plan.");
						synchronized (m_table.m_continuousConsumers) {
							m_table.m_continuousConsumers.remove(m_consumer);
						}
						synchronized (m_table.m_queries) {
							Query q = m_table.m_queries.remove(m_consumer);
							if (q != null) {
								q.m_runningQuery.abort();
							}
						}
						return Result.HARD_ERROR;
					} else {
						m_logger.warn("RemoteException " + e.getMessage() + " sending ping request to " + m_consumer + " again");
						return Result.SOFT_ERROR;
					}
				}
			} catch (RGMAPermanentException e) {
				m_logger.error("Unexpected error in pinging " + m_consumer + " " + e.getFlattenedMessage());
				return Result.HARD_ERROR;
			} catch (RGMATemporaryException e) {
				m_logger.error("Unexpected error in pinging " + m_consumer + " " + e.getFlattenedMessage());
				return Result.SOFT_ERROR;
			}
		}
	}

	/**
	 * Task which register Producer table with Registry
	 */
	private class RegistrationTask extends Task {
		private int m_hrpSecs;

		private Table m_table;

		public RegistrationTask(Table table, int hrpSecs) {
			super(m_endpoint.toString(), "Registryfor:" + table.m_vdbName, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_table = table;
			m_hrpSecs = hrpSecs;
		}

		@Override
		public Result invoke() {

			Result result = Result.SUCCESS;
			try {
				List<ConsumerEntry> consumerEntries = s_registry.registerProducerTable(m_table.m_vdbName, true, m_endpoint, m_table.m_tableName,
						m_table.m_predicate.toString(), m_table.m_producerType, m_hrpSecs, s_registryTerminationIntervalSecs);
				/* Look for any new consumers and add them to m_table.m_consumerEndpoints */
				for (ConsumerEntry consumer : consumerEntries) {
					ResourceEndpoint c = consumer.getEndpoint();
					if (!c.equals(m_endpoint)) {
						synchronized (m_table.m_continuousConsumers) {
							if (!m_table.m_continuousConsumers.containsKey(c)) {
								m_table.m_continuousConsumers.put(c, new ContinuousConsumer(c));
								if (m_logger.isDebugEnabled()) {
									m_logger.debug("New continuous consumer " + consumer + " for " + ProducerResource.this.m_endpoint);
								}
							}
						}
					}
				}

				/* Schedule a NotifyConsumerTask if needed */
				Set<ResourceEndpoint> consumerEps = new HashSet<ResourceEndpoint>();
				synchronized (m_table.m_continuousConsumers) {
					for (Entry<ResourceEndpoint, ContinuousConsumer> e : m_table.m_continuousConsumers.entrySet()) {
						ResourceEndpoint consumerEp = e.getKey();
						consumerEps.add(consumerEp);
						ContinuousConsumer consumer = e.getValue();
						if (consumer.m_task != null) {
							consumer.m_task.abort();
							consumer.m_task = null;
						}
						if (!consumer.m_connected) {
							NotifyConsumerTask notifyConsumer = new NotifyConsumerTask(m_table, consumerEp);
							s_taskInvocationQueue.add(notifyConsumer);
							consumer.m_task = notifyConsumer;
						} else {
							PingConsumableTask pingConsumer = new PingConsumableTask(m_table, consumerEp);
							s_taskInvocationQueue.add(pingConsumer);
							consumer.m_task = pingConsumer;
						}
					}
				}
				if (m_tupleStore != null) {
					m_tupleStore.updateConsumerList(m_table.m_vdbTableName, consumerEps);
				}
				long now = System.currentTimeMillis();
				synchronized (ProducerResource.this) {
					m_lastRegistryUpdate = now;
				}
				synchronized (m_table) {
					m_table.m_lastRegistryUpdate = now;
				}
				if (m_logger.isInfoEnabled()) {
					m_logger.info("Registered table " + m_table.m_vdbTableName + " with registry for producer: " + m_endpoint);
				}
			} catch (RGMAPermanentException e) {
				m_logger.error("Unexpected error in registering table", e);
				result = Result.HARD_ERROR;
			} catch (RGMATemporaryException e) {
				m_logger.warn("Failed to register table: " + e);
				result = Result.SOFT_ERROR;
			}
			return result;
		}
	}

	/**
	 * TimerTask to poll the schema for table/view changes.
	 */
	private class SchemaUpdatedTask extends TimerTask {
		@Override
		public void run() {
			List<Table> tables;
			synchronized (m_tables) {
				tables = new ArrayList<Table>(m_tables.values());
			}
			long creationTime = 0;
			for (Table t : tables) {
				try {
					creationTime = s_schema.getTableTimestamp(t.m_vdbName, t.m_tableName);
				} catch (RGMAPermanentException e) {
					m_logger.error("Error retrieving timestamp update for table \"" + t.m_vdbName + "." + t.m_tableName + "\"", e);
				}

				if (t.m_tableUpdateTime != creationTime) {
					try {
						destroy();
					} catch (RGMAPermanentException e) {
						m_logger.error(e);
					}
					break;
				}
			}
		}
	}

	/**
	 * Task which sends a removeProducer message to a remote consumable
	 */
	private class SendRemoveProducerTask extends Task {

		private final ResourceEndpoint m_consumer;

		public SendRemoveProducerTask(Table table, ResourceEndpoint consumer) {
			super(m_endpoint.toString(), consumer.getURL().toString(), s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_consumer = consumer;
		}

		@Override
		public Result invoke() {
			Result result = Result.SUCCESS;
			try {
				RemoteConsumable.removeProducer(m_consumer.getURL(), m_consumer.getResourceID(), m_endpoint);

				if (m_logger.isInfoEnabled()) {
					m_logger.info("Sent remove producer message to :" + m_consumer);
				}
			} catch (UnknownResourceException e) {
				m_logger.debug("Failed to remove producer: " + m_endpoint + " to " + m_consumer);
				result = Result.SUCCESS;
			} catch (RemoteException e) {
				m_logger.warn("Error sending remove producer request to " + m_consumer + " from " + m_endpoint + " " + e.getMessage());
				result = Result.SOFT_ERROR;
			} catch (RGMAPermanentException e) {
				m_logger.error("Error sending remove producer request to " + m_consumer + " from " + m_endpoint + " " + e.getMessage());
				result = Result.HARD_ERROR;
			} catch (RGMATemporaryException e) {
				m_logger.error("Error sending remove producer request to " + m_consumer + " from " + m_endpoint + " " + e.getMessage());
				result = Result.SOFT_ERROR;
			}
			return result;
		}
	}
}
