/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.mediator.Mediator;
import org.glite.rgma.server.services.mediator.Plan;
import org.glite.rgma.server.services.mediator.PlanEntry;
import org.glite.rgma.server.services.mediator.PlanInstruction;
import org.glite.rgma.server.services.mediator.ProducerDetails;
import org.glite.rgma.server.services.registry.RegistryService;
import org.glite.rgma.server.services.resource.Resource;
import org.glite.rgma.server.services.schema.SchemaService;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.TableNameAndAlias;
import org.glite.rgma.server.services.sql.TableReference;
import org.glite.rgma.server.services.sql.Validator;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.streaming.StreamingReceiver;
import org.glite.rgma.server.services.tasks.Task;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.Units;
import org.glite.rgma.server.system.UserContext;

/**
 * Consumer resource. Consumers run queries on behalf of a user. They are initialised with the query, query type and
 * timeout. The query is run when the start method is called and results are returned via the pop method.
 * <p>
 * If the query is still running after the timeout has elapsed, the query is aborted and no further results are
 * collected (those already stored may still be retrieved via the pop method). Queries can also be aborted explicitly
 * using the abort method. The user can check if the query was aborted by calling the hasAborted method.
 * <p>
 * Two objects are used for synchronization. The consumer instance is used to synchronize methods which change the state
 * of the consumer (NEW, STARTED, FINISHED, ABORTED) and to the m_registryUnavailable flag. <code>m_plan</code> is used
 * to synchronize methods which modify the query plan or the current list of running replies. These methods are only
 * executed after m_plan has been initialized.
 */
public class ConsumerResource extends Resource implements Consumable, Observer {
	/**
	 * States a ConsumerResource may be in
	 */
	public enum Mode {
		/** Query aborted */
		ABORTED,
		/** Query completed */
		FINISHED,
		/** Not yet started */
		NEW,
		/** Plan has been generated and producers started */
		RUNNING,
		/** Start has been called */
		STARTED,
	}

	/**
	 * Task which closes the plan and unregisters the consumer. Calls closePlan on the Mediator, which in turn calls
	 * unregisterContinuousConsumer on one or more registry replicas.
	 */
	private class ClosePlanTask extends Task {

		public ClosePlanTask() {
			super(m_endpoint.toString(), m_registryKey, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
		}

		@Override
		public Result invoke() {
			synchronized (ConsumerResource.this) {
				Result result = Result.SUCCESS;
				if (m_queryProperties.isContinuous()) {
					try {
						s_mediator.closePlan(m_endpoint, m_vdbs);
						if (m_logger.isDebugEnabled()) {
							m_logger.debug("Consumer " + m_endpoint.getResourceID() + " closed plan " + m_plan);
						}
					} catch (RGMAPermanentException e) {
						m_logger.error("RGMAPermanentException closing plan", e);
						result = Result.HARD_ERROR;

					} catch (RGMATemporaryException e) {
						m_logger.warn("Could not contact required registry to close plan: " + e.getMessage());
						result = Result.SOFT_ERROR;
					}
				}
				return result;
			}
		}
	}

	/**
	 * Task which gets the initial query plan for the consumer. Calls getPlansForQuery on the Mediator, which in turn
	 * calls getMatchingProducersForTables on one or more registry replicas. Once a plan has been obtained and selected,
	 * it is executed.
	 */
	private class GetPlansTask extends Task {

		public GetPlansTask() {
			super(m_endpoint.toString(), m_registryKey, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
		}

		@Override
		public Result invoke() {
			synchronized (ConsumerResource.this) {
				Result result = Result.SUCCESS;
				if (m_mode == Mode.STARTED) {
					try {
						List<Plan> plans = s_mediator.getPlansForQuery(m_endpoint, m_select, m_queryProperties, s_registryTerminationInterval,
								m_tablesForViews, false);
						List<Plan> plansWithoutWarning = new ArrayList<Plan>();
						List<Plan> plansWithWarning = new ArrayList<Plan>();
						for (Plan plan : plans) {
							if (!plan.hasWarning()) {
								plansWithoutWarning.add(plan);
							} else {
								plansWithWarning.add(plan);
							}
						}
						if (m_logger.isDebugEnabled()) {
							m_logger.debug("Consumer " + m_endpoint.getResourceID() + " got " + plans.size() + " plans, " + plansWithWarning.size()
									+ " with warning, " + plansWithoutWarning.size() + " without");
						}
						if (plansWithoutWarning.size() > 0) {
							m_plan = chooseRandomPlan(plansWithoutWarning);
						} else {
							m_plan = chooseRandomPlan(plansWithWarning);
						}
						executePlan();
						m_mode = Mode.RUNNING;
						m_lastRegistryUpdate = System.currentTimeMillis();
					} catch (RGMAPermanentException e) {
						m_logger.error("Unexpected error getting plans from mediator", e);
						result = Result.HARD_ERROR;
					} catch (RGMATemporaryException e) {
						m_logger.warn("Could not contact required registry to get plan from mediator: " + e.getMessage());
						result = Result.SOFT_ERROR;
						recordRegistryUnavailable();
					} catch (TimerClosedException e) {
						m_logger.debug("Timer already closed");
						result = Result.HARD_ERROR;
					}
				}
				return result;
			}
		}

		/**
		 * Choose a random plan from a list.
		 */
		private Plan chooseRandomPlan(List<Plan> plans) {
			int planNumber = (int) (Math.random() * plans.size());
			return plans.get(planNumber);
		}
	}

	/**
	 * TimerTask to poll the schema for table/view changes.
	 */
	private class IsSchemaUpdatedTask extends TimerTask {
		@Override
		public void run() {
			String[] tableNames = getTableNames();
			long creationTime = 0L;

			for (String fullTableName : tableNames) {
				String[] table = new String[2];

				int i = fullTableName.lastIndexOf(".");
				if (i < 0) {
					table[0] = "";
					table[1] = fullTableName;
				} else {
					table[0] = fullTableName.substring(0, i);
					table[1] = fullTableName.substring(++i);
				}

				try {
					creationTime = s_schema.getTableTimestamp(table[0], table[1]);
				} catch (RGMAPermanentException e) {
					String tableName = table[0].equals("") ? table[1] : table[0] + "." + table[1];
					m_logger.error("Error retrieving timestamp update for table \"" + tableName + "\"", e);
				}

				try {
					if (m_tablesUpdateTime.get(fullTableName) != creationTime) {
						abend(new RGMATemporaryException("The definition of " + fullTableName + " has changed"));
						break;
					}
				} catch (NullPointerException e) {
					m_logger.error("IsSchemaUpdatedTask has thrown a null pointer exception on " + fullTableName);
				}
			}
		}

	}

	/**
	 * TimerTask which checks if a producer being queried is still alive. If the producer is found to have died,
	 * removeProducer is called.
	 */
	private class ProducerTestTask extends TimerTask {

		private final RunningReply m_reply;

		public ProducerTestTask(RunningReply reply) {
			m_reply = reply;
		}

		@Override
		public void run() {
			if (m_reply.isActive()) {
				m_reply.testProducer();
			} else {
				cancel();
			}
		}
	}

	/**
	 * Task which refreshes the consumer's query plan. This is implemented as a RemoveInvocation since the mediator will
	 * contact registry replicas to check if any new producers have been added.
	 */
	private class RefreshPlanTask extends Task {

		public RefreshPlanTask() {
			super(m_endpoint.toString(), m_registryKey, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
		}

		@Override
		public Result invoke() {
			Result result = Result.SUCCESS;
			try {
				synchronized (ConsumerResource.this) {
					List<PlanInstruction> instructions = s_mediator.refreshPlan(m_endpoint, m_select, m_queryProperties, m_plan, s_registryTerminationInterval,
							m_tablesForViews, false);
					modifyPlan(instructions);
					m_lastRegistryUpdate = System.currentTimeMillis();
				}
			} catch (RGMAPermanentException e) {
				m_logger.error("Unexpected error refreshing plan", e);
				result = Result.HARD_ERROR;
			} catch (RGMATemporaryException e) {
				m_logger.warn("Failed to refresh plan: " + e);
				result = Result.SOFT_ERROR;
				recordRegistryUnavailable();
			} catch (TimerClosedException e) {
				m_logger.debug("Timer already closed");
				result = Result.HARD_ERROR;
			}
			return result;
		}
	}

	/**
	 * Task which removes a producer from the consumer's query plan. This is implemented as a RemoveInvocation since the
	 * mediator may contact registry replicas to patch the query plan (although this will not normally be necessary).
	 */
	private class RemoveProducerTask extends Task {

		private final ResourceEndpoint m_producer;

		public RemoveProducerTask(ResourceEndpoint producer) {
			super(m_endpoint.toString(), m_registryKey, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_producer = producer;
		}

		@Override
		public Result invoke() {
			synchronized (ConsumerResource.this) {
				Result result = Result.SUCCESS;
				try {
					List<PlanInstruction> instructions = s_mediator.removeProducerFromPlan(m_endpoint, m_select, m_queryProperties, m_plan, m_producer,
							m_tablesForViews, false);
					modifyPlan(instructions);
				} catch (RGMAPermanentException e) {
					m_logger.error("Unexpected error removing producer " + e);
					result = Result.HARD_ERROR;
				} catch (RGMATemporaryException e) {
					m_logger.warn(e.getMessage());
					result = Result.SOFT_ERROR;
				} catch (TimerClosedException e) {
					m_logger.debug("Timer already closed");
					result = Result.HARD_ERROR;
				}
				return result;
			}
		}
	}

	/**
	 * TimerTask to abort the query when it reaches its timeout.
	 */
	private class TimeoutTask extends TimerTask {
		@Override
		public void run() {
			try {
				abort();
			} catch (RGMAPermanentException e) {
				// Should never happen
				m_logger.error("Error aborting query: " + e.getMessage());
			}
		}
	}

	@SuppressWarnings("serial")
	private class TimerClosedException extends Exception {}

	/** Current best streaming protocol version. */
	private static final int CURRENT_STREAMING_PROTOCOL_VERSION = 1;

	/** Server configuration parameters. */
	private static ServerConfig s_config;

	private static int s_consumerMaxTuplesDB;

	private static int s_consumerMaxTuplesMem;

	private static int s_consumerTupleWriteBatchSize;

	private static int s_maximumTaskAttemptCount;

	private static long s_maximumTaskTimeMillis;

	/** Max. num of tuples a consumer can pop each time. */
	private static int s_maxPopTuples;

	/** Mediator component used to create and maintain query plans */
	private static Mediator s_mediator;

	/** How often to check producers are still alive in milliseconds. */
	private static long s_pingInterval;

	private static TimeInterval s_registryTerminationInterval;

	/** Connection to a schema service. */
	private static SchemaService s_schema;

	private static int s_schemaTableUpdateInSec;

	/** Number of tuples in a streamed result set */
	private static int s_streamingChunkSize;

	private static StreamingProperties s_streamingProps;

	/** Streaming receiver */
	private static StreamingReceiver s_streamingReceiver;

	/** Queue for asynchronous messaging */
	private static TaskManager s_taskInvocationQueue;

	/** When a remote exception will be treated as an unknown resource exception */
	private static long s_intervalToGiveUpOnUnreachableMillis;

	/** Query properties */
	public final QueryProperties m_queryProperties;

	/** Whether the executing query is directed or not */
	private boolean m_directed;

	/** This will be thrown at the next pop */
	private RGMAException m_exception; //

	/** Holds number of secs elapsed since last pop */
	private long m_lastPopTime;

	/** Holds the last time the registry is updated */
	private long m_lastRegistryUpdate;

	/** Current mode (new, started, aborted etc) */
	private Mode m_mode;

	/** Current query plan */
	private Plan m_plan;

	/** Current getPlans message. */
	private Task m_plansTask;

	private String m_registryKey;

	private boolean m_registryUnavailable;

	/** Mapping from plan entries to a producer reply */
	private final Map<PlanEntry, RunningReply> m_replies;

	/** Parsed SQL SELECT statement */
	private final SelectStatement m_select;

	/**
	 * System time at which the query was started. Used to modify the query interval and timeout for producers added
	 * during a continuous query
	 */
	private long m_startTime;

	/**
	 * Mapping from table name to table definition for all tables referenced in the query
	 */
	private final Map<String, SchemaTableDefinition> m_tableDefs;

	/**
	 * Mapping from view name to underlying table name for all views referenced in the query
	 */
	private final Map<String, String> m_tablesForViews;

	/** Mapping from table/view name to creation time */
	private final Map<String, Long> m_tablesUpdateTime;

	/** Query timeout, or <code>null</code> if query not started */
	private TimeInterval m_timeout;

	/**
	 * Thread responsible for timing out the query and periodically checking status of producers
	 */
	private final Timer m_Timer;

	/** Storage area for answers to the query */
	private final TupleQueue m_tupleQueue;

	private Set<String> m_vdbs;

	/** Last warning from a plan which has been returned to the user via a pop */
	private String m_poppedPlanWarning;

	static void setStaticVariables(ConsumerService service, TimeInterval registryTerminationInterval) throws RGMAPermanentException {
		Resource.setStaticVariables();
		s_config = ServerConfig.getInstance();
		s_mediator = new Mediator(RegistryService.getInstance());
		s_taskInvocationQueue = TaskManager.getInstance();
		s_streamingReceiver = StreamingReceiver.getInstance();
		s_streamingReceiver.setConsumer(service);
		s_pingInterval = s_config.getLong(ServerConstants.CONSUMER_PING_INTERVAL_SECS) * 1000;
		s_streamingChunkSize = s_config.getInt(ServerConstants.CONSUMER_MAX_TUPLE_COUNT_PER_STREAMED_CHUNK);
		s_schemaTableUpdateInSec = s_config.getInt(ServerConstants.CONSUMER_SCHEMA_CHECK_INTERVAL_SECS);
		s_schema = SchemaService.getInstance();
		s_maxPopTuples = s_config.getInt(ServerConstants.CONSUMER_MAX_POP_TUPLES);
		s_consumerMaxTuplesMem = s_config.getInt(ServerConstants.CONSUMER_MAX_TUPLES_MEM);
		s_consumerMaxTuplesDB = s_config.getInt(ServerConstants.CONSUMER_MAX_TUPLES_DB);
		s_consumerTupleWriteBatchSize = s_config.getInt(ServerConstants.CONSUMER_TUPLE_WRITE_BATCH_SIZE);
		s_maximumTaskTimeMillis = s_config.getInt(ServerConstants.CONSUMER_MAXIMUM_TASK_TIME_SECS) * 1000;
		s_maximumTaskAttemptCount = s_config.getInt(ServerConstants.RESOURCE_MAXIMUM_TASK_ATTEMPT_COUNT);
		s_registryTerminationInterval = registryTerminationInterval;
		s_streamingProps = new StreamingProperties(service.getURL().getHost(), s_streamingReceiver.getPort(), s_streamingChunkSize,
				CURRENT_STREAMING_PROTOCOL_VERSION);
		s_intervalToGiveUpOnUnreachableMillis = s_config.getLong(ServerConstants.RESOURCE_INTERVAL_TO_GIVE_UP_ON_UNREACHABLE_SECS) * 1000;
	}

	public ConsumerResource(UserContext userContext, String select, QueryProperties queryProperties, ResourceEndpoint endpoint, TimeInterval timeout,
			List<ResourceEndpoint> producers) throws RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		super(endpoint, userContext, Logger.getLogger(ConsumerConstants.CONSUMER_LOGGER));

		TimeInterval ti = queryProperties.getTimeInterval();
		if (ti != null) {
			if (ti.getValueAs(Units.MILLIS) < 0) {
				throw new RGMAPermanentException("Time interval of QueryProperty may not be negative");
			}
		}
		m_queryProperties = queryProperties;

		String qTableName = ConsumerConstants.TUPLEQUEUE_TABLENAME_PREFIX + m_endpoint.getResourceID();
		m_tupleQueue = new TupleQueue(s_consumerMaxTuplesMem, s_consumerMaxTuplesDB, s_consumerTupleWriteBatchSize, qTableName);

		m_replies = new HashMap<PlanEntry, RunningReply>();
		m_Timer = new Timer(true);

		try {
			m_select = SelectStatement.parse(select);
		} catch (ParseException e) {
			throw new RGMAPermanentException("Invalid SELECT statement: " + e.getMessage());
		}

		if (queryProperties.isContinuous()) {
			m_select.checkQueryIsSimple();
			if (m_logger.isDebugEnabled()) {
				m_logger.debug(queryProperties + " query " + m_select + " is simple in " + this);
			}
		}

		if (queryProperties.isStatic()) {
			m_select.checkQueryIsODPSimple();
			if (m_logger.isDebugEnabled()) {
				m_logger.debug(queryProperties + " query " + m_select + " is ODP simple in " + this);
			}
		}

		// Populate m_tableDefs, m_tablesForViews and m_tablesUpdateTime
		m_tableDefs = new HashMap<String, SchemaTableDefinition>();
		m_tablesForViews = new HashMap<String, String>();
		m_tablesUpdateTime = new HashMap<String, Long>();
		List<TableNameAndAlias> tables = m_select.getTables();
		m_vdbs = new HashSet<String>();
		for (TableNameAndAlias table : tables) {
			String tableName = table.getTableName();
			String vdb = table.getVdbName();
			m_vdbs.add(vdb);
			SchemaTableDefinition tableDef = s_schema.getTableDefinition(vdb, tableName, null);
			String vdbTableName = vdb + "." + tableName;
			m_tableDefs.put(vdbTableName.toUpperCase(), tableDef);
			if (tableDef.isView()) {
				m_tablesForViews.put(vdbTableName.toUpperCase(), tableDef.getViewFor());
			}

			// Adds table name and its creation time "m_tablesUpdateTime"
			m_tablesUpdateTime.put(vdbTableName, s_schema.getTableTimestamp(vdb, tableName));

		}
		new Validator(m_select, m_tableDefs).validate();

		SortedSet<String> schemaNames = new TreeSet<String>();
		for (TableReference t : m_select.getFrom()) {
			schemaNames.add(t.getTable().getVdbName());
		}
		StringBuilder registryKey = new StringBuilder("Registryfor:");
		boolean empty = true;
		for (String sn : schemaNames) {
			if (!empty) {
				registryKey.append("&");
			}
			registryKey.append(sn);
			empty = false;
		}
		m_registryKey = new String(registryKey);

		m_mode = Mode.NEW;
		m_plan = null;
		m_directed = false;
		m_plansTask = null;
		m_status = Status.ACTIVE;

		if (timeout != null && timeout.getValueAs(Units.SECONDS) <= 0) {
			throw new RGMAPermanentException("Timeout for query must be a positive whole number");
		}
		if (producers == null) {
			start(timeout);
		} else {
			start(timeout, producers);
		}
	}

	/**
	 * Store the exception to throw on a subsequent pop - after which the consumer will destory itself
	 */
	public synchronized void abend(RGMAException e) {
		m_exception = e;
	}

	/**
	 * Abort the currently running query Synchronized on instance since this involves a state change.
	 * 
	 * @throws RGMAPermanentException
	 */
	public synchronized void abort() throws RGMAPermanentException {
		if (m_mode == Mode.NEW) {
			throw new RGMAPermanentException("Query has not been started");
		}
		if (m_mode == Mode.FINISHED || m_mode == Mode.ABORTED) {
			return;
		}
		removeReplies();
		m_mode = Mode.ABORTED;
	}

	/**
	 * Handle an addProducer message
	 * 
	 * @throws RGMAPermanentException
	 */
	public synchronized void addProducer(ProducerTableEntry producer) throws RGMAPermanentException {
		if (!m_queryProperties.isContinuous()) {
			throw new RGMAPermanentException("Can not add producer to non-continuous consumer.");
		}
		synchronized (this) {
			if (m_mode != Mode.RUNNING) {
				if (m_logger.isDebugEnabled()) {
					m_logger.debug("Add producer: " + producer + " to " + this + " has no effect in Mode: " + m_mode);
				}
				return;
			}
		}
		try {
			List<PlanInstruction> instructions = s_mediator.addProducerToPlan(m_endpoint, m_select, m_queryProperties, m_plan, producer);
			modifyPlan(instructions);
		} catch (TimerClosedException e) {
			m_logger.debug("Timer already closed");

		}
	}

	/**
	 * Closed consumers can be destroyed without any warning.
	 */
	@Override
	public boolean canDestroy() {
		synchronized (m_status) {
			return m_status == Status.CLOSED;
		}
	}

	/**
	 * Destroy the consumer. Synchronized on instance since it involves a state change.
	 */
	@Override
	public synchronized void destroy() {
		synchronized (m_status) {
			if (m_status == Status.DESTROYED) {
				return;
			}
		}
		try {
			if (m_mode != Mode.NEW) {
				removeReplies();
			}
			if (m_plan != null) {
				if (m_plansTask != null) {
					m_plansTask.abort();
				}
				s_taskInvocationQueue.add(new ClosePlanTask());
			}
			m_tupleQueue.close();
		} catch (Exception e) {
			m_logger.warn("Error destroying consumer resource", e);
		} finally {
			m_Timer.cancel();
			synchronized (m_status) {
				m_status = Status.DESTROYED;
			}
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Destroyed consumer " + m_endpoint.getResourceID());
		}
	}

	/**
	 * Test if the query has aborted.
	 * 
	 * @return <code>true</code> if the query was aborted or timed out, <code>false</code> otherwise.
	 */
	public synchronized boolean hasAborted() {
		return m_mode == Mode.ABORTED;
	}

	/**
	 * Pop results of the query.
	 * 
	 * @param context
	 *            Security context
	 * @param count
	 *            Maximum number of tuples that may be returned.
	 * @return ResultSet containing at most <code>count</code> tuples. The endOfResults flag is set if and only if the
	 *         query has completed (or been aborted) and no more results are available to pop.
	 * @throws RGMAPermanentException
	 */
	public synchronized TupleSet pop(UserContext context, int count) throws RGMATemporaryException, RGMAPermanentException {

		checkContext(context);

		if (m_exception != null) {
			destroy();
			if (m_exception instanceof RGMATemporaryException) {
				throw (RGMATemporaryException) m_exception;
			} else {
				throw (RGMAPermanentException) m_exception;
			}
		}

		if (m_mode == Mode.NEW) {
			throw new RGMAPermanentException("Query has not been started");
		}
		if (count < 0) {
			throw new RGMAPermanentException("Cannot pop " + count + " number of tuples");
		}

		/* Reduce count if it is greater than maximum */
		if (count > s_maxPopTuples) {
			count = s_maxPopTuples;
		}

		TupleSet rs = m_tupleQueue.pop(count);
		m_lastPopTime = System.currentTimeMillis();

		StringBuilder warning = new StringBuilder();
		if (m_plan != null) {
			if (!m_plan.getWarning().equals(m_poppedPlanWarning)) {
				warning.append(m_plan.getWarning());
				m_poppedPlanWarning = m_plan.getWarning();
			}
		}
		synchronized (this) {
			if (m_registryUnavailable) {
				m_registryUnavailable = false;
				if (warning.length() > 0) {
					warning.append(' ');
				}
				warning.append("Information may be missing as the registry was not available for some time");
			}
		}
		if (warning.length() > 0) {
			rs.setWarning(warning.toString());
		}
		/* Set end of results if query has finished and all tuples have been popped. */
		checkQueryFinished();
		if ((m_mode == Mode.FINISHED || m_mode == Mode.ABORTED) && m_tupleQueue.isEmpty()) {
			rs.setEndOfResults(true);
		}
		return rs;
	}

	/**
	 * Add tuples to the TupleQueue to be popped by the user.
	 */
	public void push(TupleSet rs, String dummy) {
		try {
			m_tupleQueue.push(rs);
		} catch (RGMAPermanentException e) {
			abend(e);
		}
	}

	/**
	 * Handle a removeProducer message.
	 */
	public void removeProducer(ResourceEndpoint producer) {
		synchronized (this) {
			if (m_mode != Mode.RUNNING) {
				if (m_logger.isDebugEnabled()) {
					m_logger.debug("Remove producer: " + producer + " from consumer resource: " + m_endpoint.getResourceID() + " has no effect in Mode: "
							+ m_mode);
				}
				return;
			}
		}

		if (m_directed) {
			synchronized (m_plan) {
				Iterator<PlanEntry> peIter = m_plan.getPlanEntries().iterator();
				while (peIter.hasNext()) {
					PlanEntry entry = peIter.next();
					if (entry.getProducer().getEndpoint().equals(producer)) {
						RunningReply reply;
						synchronized (m_replies) {
							reply = m_replies.remove(entry);
						}
						reply.abort();
						s_streamingReceiver.removeReply(reply);
						peIter.remove();
						if (m_logger.isDebugEnabled()) {
							m_logger.debug("Removed producer: " + producer + " from consumer resource: " + m_endpoint.getResourceID());
						}
					}
				}
			}
		} else {
			// Sent asynchronously because it may contact the Registry.
			Task removeProducer = new RemoveProducerTask(producer);
			s_taskInvocationQueue.add(removeProducer);

			if (m_logger.isDebugEnabled()) {
				m_logger.debug("Queued RemoveProducer for consumer " + m_endpoint.getResourceID() + " and producer " + producer);
			}
		}
	}

	/**
	 * Callback for each producer reply after the table timestamp information is received. If there is no mismatch in
	 * timestamp operation proceeds normally; otherwise, this consumer unregisters the producer reply that invoked this
	 * callback to avoiding receiving inconsistent data. The plan is not modified as we never want to hear from that
	 * producer again and if it were removed from the plan it would be tried again.
	 */
	public void update(Observable observable, Object obj) {

		TupleSet rs = (TupleSet) obj;
		RunningReply reply = (RunningReply) observable;
		String vdbTableName;
		long timeStamp = 0;

		for (String[] row : rs.getData()) {
			vdbTableName = row[0];
			timeStamp = Long.parseLong(row[1]);
			Long timeCreated = m_tablesUpdateTime.get(vdbTableName);
			if (timeCreated == null) {
				m_logger.error("Unexpected table returned " + vdbTableName + " in reply to start");
			} else if (timeCreated != timeStamp) {
				m_logger.warn("Timestamp for creation of table " + vdbTableName + " in " + this + "  (" + timeCreated
						+ ") does not match the contacted producer (" + timeStamp + ")");
				reply.abort();
				s_streamingReceiver.removeReply(reply);
				break;
			}
		}
	}

	/**
	 * Update the consumer's entry in the registry.
	 */
	@Override
	public synchronized void updateRegistry() {
		if (m_logger.isDebugEnabled()) {
			m_logger.debug("UpdateRegistry called in mode " + m_mode + " for " + m_endpoint.getResourceID());
		}

		if (!m_directed) {
			if (m_mode == Mode.STARTED) {
				/* Ensures this consumer recovers from a GetPlans task that fails */
				if (m_plansTask != null) {
					m_plansTask.abort();
				}
				m_plansTask = new GetPlansTask();
				s_taskInvocationQueue.add(m_plansTask);

			} else if (m_mode == Mode.RUNNING) {

				if (m_plansTask != null) {
					m_plansTask.abort();
				}

				m_plansTask = new RefreshPlanTask();
				s_taskInvocationQueue.add(m_plansTask);
			}

			if (m_logger.isDebugEnabled()) {
				m_logger.debug("Queued registry update for consumer " + m_endpoint.getResourceID());
			}
		}
	}

	/**
	 * Check if the query has completed normally. This is only relevant to one-time queries and checks if all of the
	 * producers contacted have send an endOfResults flag.
	 */
	private synchronized void checkQueryFinished() {

		// Continuous queries only finish when they are aborted or time out.
		if (m_queryProperties.isContinuous()) {
			return;
		}

		// If we don't have a plan yet, the query cannot have been completed.
		if (m_mode == Mode.RUNNING) {
			int numComplete = 0;

			synchronized (m_replies) {
				for (RunningReply reply : m_replies.values()) {
					if (!reply.isActive()) {
						numComplete++;
					}
				}
				m_logger.debug(this + " has " + numComplete + " endOfResults flags received from all producers out of " + m_replies.size() + " replies");
				if (numComplete == m_replies.size()) {
					m_mode = Mode.FINISHED;
				}
			}
		}
	}

	/**
	 * Execute the query plan
	 * 
	 * @throws TimerClosedException
	 */
	private void executePlan() throws TimerClosedException {
		synchronized (m_plan) {
			for (PlanEntry entry : m_plan.getPlanEntries()) {
				if (m_logger.isDebugEnabled()) {
					m_logger.debug("Executing plan entry: " + entry);
				}
				executePlanEntry(entry);
			}
		}
	}

	/**
	 * Execute a PlanEntry.
	 * 
	 * @throws TimerClosedException
	 */
	private void executePlanEntry(PlanEntry entry) throws TimerClosedException {
		QueryProperties props = null;
		TimeInterval timeout = m_timeout;
		if (m_queryProperties.isContinuous()) {
			/* Modify the query timeout and time interval */
			long querySecondsElapsed = (System.currentTimeMillis() - m_startTime) / 1000;
			if (m_timeout != null) {
				long timeoutSec = m_timeout.getValueAs(Units.SECONDS) - querySecondsElapsed;

				/* Don't start reply if query has already timed out */
				if (timeoutSec <= 0) {
					/*
					 * Query is about to be terminated so don't bother revising the plan
					 */
					return;
				}
				timeout = new TimeInterval(timeoutSec, Units.SECONDS);
			}
			long queryIntervalSec = 0;
			if (m_queryProperties.hasTimeInterval()) {
				queryIntervalSec = m_queryProperties.getTimeInterval().getValueAs(Units.SECONDS) + querySecondsElapsed;
			} else {
				queryIntervalSec = querySecondsElapsed;
			}

			if (queryIntervalSec > 0) {
				props = QueryProperties.getContinuous(new TimeInterval(queryIntervalSec, Units.SECONDS));
			} else {
				props = QueryProperties.CONTINUOUS;
			}
		} else {
			props = m_queryProperties;
		}

		/* Create a NormalReply and deal with it */
		RunningReply reply = new RunningReply(entry, this, s_taskInvocationQueue, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount, s_intervalToGiveUpOnUnreachableMillis, null);
		s_streamingReceiver.addReply(reply);

		/* This block is to ensure that if a reply on the queue is inactive then it has completed */
		synchronized (m_replies) {
			m_replies.put(entry, reply);
			reply.start(timeout, props, s_streamingProps, m_context);
		}

		/*
		 * Register the callback method "update" on consumer resource with the producer reply "reply"
		 */
		reply.addObserver(this);

		/* Check the producer periodically */
		try {
			synchronized (m_status) {
				if (m_status != Status.DESTROYED) {
					m_Timer.schedule(new ProducerTestTask(reply), s_pingInterval, s_pingInterval);
				}
			}
		} catch (IllegalStateException e) {
			/* This can happen if the resource has been destroyed */
			throw new TimerClosedException();
		}
	}

	/**
	 * @return A detailed summary of the producers being queried (in XML).
	 */
	private String getProducerDetailsDisplay() {
		StringBuilder b = new StringBuilder();
		List<PlanEntry> planEntry = getConnectedProducers();

		if (planEntry.size() == 0) {
			return "";
		}

		for (PlanEntry entry : planEntry) {
			ResourceEndpoint endPoint = entry.getProducer().getEndpoint();
			b.append("<Producer ID=\"").append(endPoint.getResourceID()).append("\" ");
			b.append("URL=\"").append(endPoint.getURL()).append("\" />\n");
		}

		return b.toString();
	}

	/**
	 * @return A summary of all tables being queried (in XML).
	 */
	private String getTableNamesDisplay() {
		StringBuilder b = new StringBuilder();
		String[] tables = getTableNames();
		for (int i = 0; i < tables.length; i++) {
			b.append("<Table Name=\"").append(tables[i]).append("\"/>\n");
		}
		return b.toString();
	}

	/**
	 * Modify the query plan. Synchronized on m_plan to prevent two calls modifying the plan simultaneously
	 * 
	 * @throws TimerClosedException
	 */
	private synchronized void modifyPlan(List<PlanInstruction> instructions) throws TimerClosedException {

		if (m_mode == Mode.RUNNING) {
			synchronized (m_plan) {
				for (PlanInstruction instruction : instructions) {
					if (instruction.getType() == PlanInstruction.Type.ADD) {
						ProducerDetails producer = instruction.getProducer();
						SelectStatement select = instruction.getSelect();
						PlanEntry entry = new PlanEntry(producer, select);
						List<PlanEntry> entries = m_plan.getPlanEntries();

						if (!entries.contains(entry)) {
							/* Prevent addition of a previously added entry (Producer) */
							m_plan.getPlanEntries().add(entry);

							if (instruction.hasWarning()) {
								m_plan.setWarning(instruction.getWarning());
							}
							executePlanEntry(entry);
						}
					} else if (instruction.getType() == PlanInstruction.Type.REMOVE) {
						if (instruction.hasWarning()) {
							m_plan.setWarning(instruction.getWarning());
						}
						Iterator<PlanEntry> peIter = m_plan.getPlanEntries().iterator();
						while (peIter.hasNext()) {
							/* Need iterator because of the remove from the list */
							PlanEntry entry = peIter.next();
							if (entry.getProducer().equals(instruction.getProducer())) {
								RunningReply reply;
								synchronized (m_replies) {
									reply = m_replies.remove(entry);
								}
								reply.abort();
								s_streamingReceiver.removeReply(reply);
								peIter.remove();
							}
						}
					}
				}
			}
		}
	}

	private synchronized void recordRegistryUnavailable() {
		m_registryUnavailable = true;
	}

	private void removeReplies() {
		synchronized (m_replies) {
			for (RunningReply reply : m_replies.values()) {
				reply.abort();
				s_streamingReceiver.removeReply(reply);
			}
		}
	}

	/**
	 * Start a mediated query. Synchronized on instance since this involves a state change. The retrieval and execution
	 * of the query plan is handled asynchronously since this involves a remote Registry call.
	 */
	private void start(TimeInterval timeout) throws RGMAPermanentException, RGMAPermanentException {
		synchronized (this) {
			if (m_mode == Mode.STARTED || m_mode == Mode.RUNNING) {
				if (m_directed) {
					throw new RGMAPermanentException("Query was already started with a list of producers specified");
				}
				return;
			}
			if (m_mode == Mode.FINISHED || m_mode == Mode.ABORTED) {
				throw new RGMAPermanentException("Start called for query that has already ended");
			}
			m_startTime = System.currentTimeMillis();
			m_plansTask = new GetPlansTask();
			m_mode = Mode.STARTED;
			s_taskInvocationQueue.add(m_plansTask);
			m_directed = false;
			m_timeout = timeout;
			synchronized (m_status) {
				if (m_status != Status.DESTROYED) {
					if (timeout != null) {
						m_Timer.schedule(new TimeoutTask(), timeout.getValueAs(Units.SECONDS) * 1000);
					}
					m_Timer.schedule(new IsSchemaUpdatedTask(), s_schemaTableUpdateInSec * 1000, s_schemaTableUpdateInSec * 1000);
				}
			}
		}
	}

	/**
	 * Start a directed query Synchronized on instance since this involves a state change.
	 */
	private void start(TimeInterval timeout, List<ResourceEndpoint> producers) throws RGMAPermanentException, RGMATemporaryException {
		synchronized (this) {
			if (m_mode == Mode.STARTED || m_mode == Mode.RUNNING) {
				// only allow start to be called twice with identical paramters
				if (!m_directed) {
					throw new RGMAPermanentException("Query was already started with no list of producers specified");
				} else {
					int i = 0;
					if (m_plan.getPlanEntries().size() == producers.size()) {
						boolean same = true;
						for (PlanEntry entry : m_plan.getPlanEntries()) {
							if (!entry.getProducer().getEndpoint().equals(producers.get(i))) {
								same = false;
								break;
							}
							i++;
						}
						if (same) {
							return;
						}
					}
					throw new RGMAPermanentException("Query was already started with different list of producers");
				}
			}

			if (m_mode == Mode.FINISHED || m_mode == Mode.ABORTED) {
				throw new RGMAPermanentException("Start called for query that has already ended");
			}

			List<PlanEntry> entries = new ArrayList<PlanEntry>();
			for (ResourceEndpoint producer : producers) {
				ProducerDetails producerDetails = new ProducerDetails(producer, null, null);
				entries.add(new PlanEntry(producerDetails, m_select));
			}

			m_plan = new Plan(entries, "");
			m_directed = true;
			m_timeout = timeout;
			synchronized (m_status) {
				if (m_status != Status.DESTROYED) {
					if (timeout != null) {
						m_Timer.schedule(new TimeoutTask(), timeout.getValueAs(Units.SECONDS) * 1000);
					}
					m_Timer.schedule(new IsSchemaUpdatedTask(), s_schemaTableUpdateInSec * 1000);
				}
			}
			m_startTime = System.currentTimeMillis();
			try {
				executePlan();
			} catch (TimerClosedException e) {
				/*
				 * As start() cannot be called on a destroyed resource. The start() call will have sent a keep alive
				 * message and while executing the start() code will have received the destroy() call from the
				 * ResourceManager.
				 */
				throw new RGMATemporaryException("Resource timed out while start() was being called");
			}
			m_mode = Mode.RUNNING;
			if (m_logger.isDebugEnabled()) {
				m_logger.debug("Consumer " + m_endpoint.getResourceID() + " started directed query using plan: " + m_plan.getPlanEntries());
			}
		}
	}

	/** Returns a list of producers connected to this consumer */
	List<PlanEntry> getConnectedProducers() {
		List<PlanEntry> prod = null;
		try {
			synchronized (m_plan) {
				prod = m_plan.getPlanEntries();
			}
		} catch (NullPointerException ne) {
			prod = new ArrayList<PlanEntry>();
		}

		return prod;
	}

	/**
	 * @return A detailed summary of this particular resource.
	 */
	String getDetails(boolean fullDetails) {
		StringBuilder b = new StringBuilder();
		b.append("<Resource ID=\"");
		b.append(getEndpoint().getResourceID());
		b.append("\" ClientHostName=\"");
		b.append(getClientHostName());
		b.append("\" QueryType=\"");
		b.append(m_queryProperties);
		b.append("\" UserLastContactIntervalMillis=\"");
		b.append(getLastContactIntervalMillis());
		b.append("\" LastRegistryUpdateIntervalMillis=\"");
		b.append(lastRegistryUpdateIntervalMillis());
		b.append("\" NumTuplesInMemory=\"");
		b.append(getNumTuplesInMemory());
		b.append("\" NumTuplesOnDisk=\"");
		b.append(getNumTuplesOnDisk());
		b.append("\" LastPopIntervalMillis=\"");
		b.append(getLastPopIntervalMillis());
		b.append("\" TerminationIntervalMillis=\"");
		b.append(getTerminationInterval() * 1000);
		b.append("\" ResourceCreationTimeMillis=\"");
		b.append(getTimeCreated());
		b.append("\" ConnectedProducerCount=\"").append(getConnectedProducers().size());
		b.append("\" Status=\"").append(displayStatus());
		b.append("\">\n");
		b.append(getTableNamesDisplay());
		if (fullDetails) {
			b.append(getTasksDisplay());
			b.append(getProducerDetailsDisplay());
		}
		b.append("</Resource>\n");
		return b.toString();
	}

	/** Returns the time in ms since tuples were last popped */
	String getLastPopIntervalMillis() {

		if (m_lastPopTime == 0) {
			return "Not yet";
		} else {
			return String.valueOf(System.currentTimeMillis() - m_lastPopTime);
		}
	}

	String getNumTuplesInMemory() {
		return String.valueOf(m_tupleQueue.numTuplesMem());
	}

	int getNumTuplesOnDisk() {
		return m_tupleQueue.numTuplesDB();
	}

	/** Returns an array of table names that the consumer query specifies */
	String[] getTableNames() {

		String[] tables = new String[m_tableDefs.size() + m_tablesForViews.size()];
		Set<String> tableNames = m_tableDefs.keySet();
		tableNames.addAll(m_tablesForViews.keySet());

		return tableNames.toArray(tables);
	}

	/**
	 * Returns the time in milliseconds since the registry was last updated or a message if never updated. The result is
	 * returned as a String.
	 */
	String lastRegistryUpdateIntervalMillis() {
		synchronized (ConsumerResource.this) {
			if (m_lastRegistryUpdate == 0) {
				return "Not updated";
			} else {
				return String.valueOf(System.currentTimeMillis() - m_lastRegistryUpdate);
			}
		}
	}
}