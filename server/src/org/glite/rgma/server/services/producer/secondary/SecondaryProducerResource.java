package org.glite.rgma.server.services.producer.secondary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.TimerTask;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.consumer.Consumable;
import org.glite.rgma.server.services.consumer.RunningReply;
import org.glite.rgma.server.services.database.SQLTypeAdjuster;
import org.glite.rgma.server.services.mediator.Mediator;
import org.glite.rgma.server.services.mediator.Plan;
import org.glite.rgma.server.services.mediator.PlanEntry;
import org.glite.rgma.server.services.mediator.PlanInstruction;
import org.glite.rgma.server.services.mediator.ProducerDetails;
import org.glite.rgma.server.services.producer.ProducerResource;
import org.glite.rgma.server.services.producer.store.TupleStore.BufferFullException;
import org.glite.rgma.server.services.registry.RegistryService;
import org.glite.rgma.server.services.schema.SchemaService;
import org.glite.rgma.server.services.sql.InsertStatement;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.TableName;
import org.glite.rgma.server.services.sql.DataType.Type;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.streaming.StreamingReceiver;
import org.glite.rgma.server.services.tasks.Task;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.system.ProducerProperties;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.SchemaColumnDefinition;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.Units;
import org.glite.rgma.server.system.UserContext;

/**
 * The secondary producer resource This has similarities with the primary producer (it extends the producer resource)
 * but it also has code which is similar to the consumer resource. For each table a plan is established by the
 * GetPlansTask and that plan is periodically updated by the RefreshPlanTask
 */
public class SecondaryProducerResource extends ProducerResource implements Consumable, Observer {

	static StreamingProperties s_streamingProps;

	/** Current best streaming protocol version. */
	private static final int CURRENT_STREAMING_PROTOCOL_VERSION = 1;

	/** Server configuration parameters. */
	private static ServerConfig s_config;

	private static int s_maximumTaskAttemptCount;

	private static long s_maximumTaskTimeMillis;

	/** Mediator component used to create and maintain query plans */
	private static Mediator s_mediator;

	/** Secondary producer does not deal with views for secondary consumers */
	private final static Map<String, String> s_noTablesForViews = new HashMap<String, String>();

	/** How often to check producers are still alive in milliseconds. */
	private static long s_pingInterval;

	private static TimeInterval s_registryTerminationInterval;

	/** Streaming receiver */
	private static StreamingReceiver s_streamingReceiver;

	private static int s_tupleInsertIntervalMemoryCheck;

	/** When a remote exception will be treated as an unknown resource exception */
	private static long s_intervalToGiveUpOnUnreachableMillis;

	static void setStaticVariables(SecondaryProducerService service, TimeInterval registryTerminationInterval) throws RGMAPermanentException {
		String hostname = service.getURL().getHost();
		ProducerResource.setStaticVariables(registryTerminationInterval, hostname);
		s_config = ServerConfig.getInstance();
		s_mediator = new Mediator(RegistryService.getInstance());
		s_taskInvocationQueue = TaskManager.getInstance();
		s_streamingReceiver = StreamingReceiver.getInstance();
		s_streamingReceiver.setSecondaryProducer(service);
		s_pingInterval = s_config.getLong(ServerConstants.CONSUMER_PING_INTERVAL_SECS) * 1000;
		s_schema = SchemaService.getInstance();
		s_maximumTaskTimeMillis = s_config.getInt(ServerConstants.CONSUMER_MAXIMUM_TASK_TIME_SECS) * 1000;
		s_maximumTaskAttemptCount = s_config.getInt(ServerConstants.RESOURCE_MAXIMUM_TASK_ATTEMPT_COUNT);
		s_registryTerminationInterval = registryTerminationInterval;
		s_tupleInsertIntervalMemoryCheck = s_config.getInt(ServerConstants.SECONDARY_PRODUCER_COUNT_OF_TUPLES_BETWEEN_MEMORY_CHECKS);
		int streamingChunkSize = s_config.getInt(ServerConstants.CONSUMER_MAX_TUPLE_COUNT_PER_STREAMED_CHUNK);
		s_streamingProps = new StreamingProperties(hostname, s_streamingReceiver.getPort(), streamingChunkSize, CURRENT_STREAMING_PROTOCOL_VERSION);
		s_intervalToGiveUpOnUnreachableMillis = s_config.getLong(ServerConstants.RESOURCE_INTERVAL_TO_GIVE_UP_ON_UNREACHABLE_SECS) * 1000;
	}

	/** Mapping from plan entries to a producer reply */
	private final Map<PlanEntry, RunningReply> m_replies;

	private int m_tupleInsertCounter;

	SecondaryProducerResource(UserContext userContext, ProducerProperties properties, ResourceEndpoint endpoint) throws RGMAPermanentException,
			RGMAPermanentException {
		super(endpoint, userContext, properties, Logger.getLogger(SecondaryProducerConstants.SECONDARY_PRODUCER_LOGGER));
		m_replies = new HashMap<PlanEntry, RunningReply>();
		m_status = Status.ACTIVE;
	}

	/**
	 * Called from a task to terminate the SP.
	 */
	public void abend(RGMAException e) {
		m_logger.warn(e.getFlattenedMessage());
		destroy();
	}

	/**
	 * Closed secondary producers can be destroyed immediately
	 */
	@Override
	public boolean canDestroy() {
		synchronized (m_status) {
			return m_status == Status.CLOSED;
		}
	}

	/**
	 * Destroy the Secondary Producer.
	 */
	@Override
	public synchronized void destroy() {
		synchronized (m_status) {
			if (m_status == Status.DESTROYED) {
				return;
			}
		}
		try {
			super.destroy();
			List<Table> tempList;
			/* Copy the list to avoid synchronization problems with push() */
			synchronized (m_tables) {
				tempList = new ArrayList<Table>(m_tables.values());
			}
			for (Table t : tempList) {
				SecondaryProducerTable spt = (SecondaryProducerTable) t;
				Plan plan;
				Task planTask;
				synchronized (t) {
					plan = spt.m_plan;
					planTask = spt.m_planTask;
				}
				if (planTask != null) {
					planTask.abort();
				}
				synchronized (m_replies) {
					for (RunningReply reply : m_replies.values()) {
						reply.abort();
						s_streamingReceiver.removeReply(reply);
					}
				}
				if (plan != null) {
					s_taskInvocationQueue.add(new ClosePlanTask(plan, spt.m_vdbName));
				}
			}
		} catch (Exception e) {
			m_logger.warn("Error destroying secondary producer resource", e);
		} finally {
			synchronized (this) {
				synchronized (m_status) {
					m_status = Status.DESTROYED;
				}
			}
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Destroyed secondary producer " + m_endpoint.getResourceID());
		}
	}

	/**
	 * Add tuples to the TupleStore.
	 */
	public void push(TupleSet rs, String vdbTableName) {
		SecondaryProducerTable spt = null;
		try {
			spt = null;
			synchronized (m_tables) {
				spt = (SecondaryProducerTable) m_tables.get(vdbTableName);
			}
			if (spt == null) {
				throw new RGMAPermanentException("Secondary Producer push does not know table " + vdbTableName);
			}
			String insertStringPrefix = spt.m_insertStringPrefix;
			List<String[]> data = rs.getData();
			for (String[] row : data) {
				try {
					StringBuffer sb = new StringBuffer(insertStringPrefix);
					int i = 0;
					for (String colValue : row) {
						if (colValue == null) {
							sb.append("NULL");
						} else {
							/* Ensure that all fields are good */
							Type type = spt.m_types.get(i);
							if (type == Type.TIMESTAMP) {
								sb.append("'" + SQLTypeAdjuster.checkTimestamp(colValue) + "'");
							} else if (type == Type.DATE) {
								sb.append("'" + SQLTypeAdjuster.checkDate(colValue) + "'");
							} else if (type == Type.TIME) {
								sb.append("'" + SQLTypeAdjuster.checkTime(colValue) + "'");
							} else if (type == Type.CHAR || type == Type.VARCHAR) {
								sb.append("'" + colValue.replace("'", "''") + "'");
							} else {
								sb.append(colValue);
							}
						}
						if (i < row.length - 1) {
							sb.append(",");
						} else {
							sb.append(")");
						}
						i++;
					}
					if (m_tupleInsertCounter++ % s_tupleInsertIntervalMemoryCheck == 0) {
						/*
						 * Note that we don't call checkBusy here but just ensure that we have the memory to continue
						 */
						try {
							Service.checkMemoryLow();
						} catch (RGMATemporaryException e) {
							logReject(spt, e);
							m_logger.error("Memory is low for secondary producer " + m_endpoint + ". Closing it down");
							try {
								close();
							} catch (RGMAPermanentException e1) {
								m_logger.error("Failed to close secondary producer " + m_endpoint + "!");
							}
							return;
						}
					}
					InsertStatement tuple = InsertStatement.parse(sb.toString());
					m_tupleStore.insert(m_context, tuple);
					synchronized (spt) {
						spt.m_lastInsertTime = System.currentTimeMillis();
						spt.m_totalInsertedTuples++;
					}
					/* Can only log exceptions as nobody is listening */
				} catch (RGMAPermanentException e) {
					logReject(spt, e);
					m_logger.error("Something went wrong ", e);
				} catch (BufferFullException e) {
					logReject(spt, e);
					m_logger.error("Buffer is full for secondary producer " + m_endpoint + ". Closing it down");
					try {
						close();
					} catch (RGMAPermanentException e1) {
						m_logger.error("Failed to close secondary producer " + m_endpoint + "!");
					}
					return;
				} catch (ParseException e) {
					logReject(spt, e);
					m_logger.error(e);
				}
			}
			/* Can only log exceptions as nobody is listening */
		} catch (RGMAPermanentException e) {
			logReject(spt, e);
			m_logger.error(e);
		}
	}

	/**
	 * Handle a removeProducer message.
	 */
	public synchronized void removeProducer(ResourceEndpoint producer) {
		synchronized (m_tables) {
			for (Table t : m_tables.values()) {
				SecondaryProducerTable spt = (SecondaryProducerTable) t;
				Task removeProducer = new RemoveProducerTask(producer, spt);
				s_taskInvocationQueue.add(removeProducer);
			}
		}
	}

	/**
	 * Callback for each producer reply after the table timestamp information is received. If there is no mismatch in
	 * timestamp operation proceeds normally; otherwise, this secondary producer unregisters the producer reply that
	 * invoked this callback to avoiding receiving inconsistent data. The code below has a loop over the entries in the
	 * result set. As a secondary prodcuer there should be exactly one.
	 */
	public void update(Observable observable, Object obj) {

		TupleSet rs = (TupleSet) obj;
		RunningReply reply = (RunningReply) observable;
		String vdbTableName;
		long timeStamp = 0;

		for (String[] row : rs.getData()) {
			vdbTableName = row[0];
			timeStamp = Long.parseLong(row[1]);
			Table table;
			synchronized (m_tables) {
				table = m_tables.get(vdbTableName);
			}
			if (table == null) {
				m_logger.error("Unexpected table returned " + vdbTableName + " in reply to start");
			} else if (table.m_tableUpdateTime != timeStamp) {
				reply.abort();
				s_streamingReceiver.removeReply(reply);
				break;
			}
		}
	}

	/**
	 * Update the secondary producer entry in the registry.
	 */
	@Override
	public void updateRegistry() {
		try {
			synchronized (m_status) {
				if (m_status == Status.DESTROYED) {
					return;
				}
			}
			synchronized (m_tables) {
				for (Table t : m_tables.values()) {
					SecondaryProducerTable table = (SecondaryProducerTable) t;
					Plan plan;
					Task planTask;
					synchronized (table) {
						plan = table.m_plan;
						planTask = table.m_planTask;
					}
					if (planTask != null) {
						planTask.abort();
					}
					if (plan == null) {
						planTask = new GetPlansTask(table);
						s_taskInvocationQueue.add(planTask);
					} else {
						planTask = new RefreshPlanTask(table);
						s_taskInvocationQueue.add(planTask);
					}
					synchronized (table) {
						table.m_planTask = planTask;
					}

					int hrpSecs = Math.min(table.m_hrpSecs, (int) ((System.currentTimeMillis() - table.m_startTimeMillis) / 1000L));
					registerTable(table, hrpSecs);
					if (m_logger.isInfoEnabled()) {
						m_logger.info("Queued update registry for SecondaryProducerResource " + m_endpoint.getResourceID() + " for table :"
								+ table.m_vdbTableName);
					}
				}
			}
		} catch (RGMAPermanentException e) {
			m_logger.error("Updated registry for Secondary Producer :" + m_endpoint.getResourceID() + " Failed");
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Queued registry update for Secondary Producer " + m_endpoint.getResourceID());
		}
	}

	/**
	 * Handle an addProducer message. If no plan exists then do nothing.
	 * 
	 * @throws RGMAPermanentException
	 */
	synchronized void addProducer(ProducerTableEntry producer) throws RGMAPermanentException {
		String vdbTableName = producer.getVdbTableName();
		if (producer.getProducerType().isSecondary()) {
			throw new RGMAPermanentException("Rejecting addProducer of " + producer + " as it from a secondary producer");
		}
		SecondaryProducerTable table;
		synchronized (m_tables) {
			table = (SecondaryProducerTable) m_tables.get(vdbTableName);
		}
		if (table == null) {
			throw new RGMAPermanentException(vdbTableName + " is not known to the Secondary Producer");
		}
		Plan plan;
		synchronized (table) {
			plan = table.m_plan;
		}
		if (plan != null) {
			List<PlanInstruction> instructions = s_mediator.addProducerToPlan(m_endpoint, table.m_select, QueryProperties.CONTINUOUS, plan, producer);
			modifyPlan(instructions, plan, table.m_startTimeMillis);
			if (m_logger.isDebugEnabled()) {
				m_logger.debug("Secondary Producer " + m_endpoint.getResourceID() + " has added new producers : " + producer + " into its plan");
			}
		}
	}

	/**
	 * Adds a table to the list of tables to which this producer may publish tuples.
	 */
	void declareTable(UserContext context, String userTableName, String predicateString, int hrpSecs) throws RGMAPermanentException, RGMAPermanentException,
			RGMAPermanentException {
		checkContext(context);
		TableName ctn = new TableName(userTableName);
		String vdbName = ctn.getVdbName();
		if (vdbName == null) {
			throw new RGMAPermanentException("Declared table must include the vdb prefix");
		}
		String vdbTableName = ctn.getVdbTableName();
		SecondaryProducerTable t;
		synchronized (m_tables) {
			t = (SecondaryProducerTable) m_tables.get(vdbTableName);
		}
		if (t != null) {
			if (t.m_hrpSecs != hrpSecs) {
				throw new RGMAPermanentException("Table " + userTableName + " has already been declared with different HRP value");
			}
			return;
		}
		SelectStatement select = null;
		StringBuilder columns = new StringBuilder();
		boolean first = true;
		List<Type> types = new ArrayList<Type>();
		SchemaTableDefinition tableDef = s_schema.getTableDefinition(vdbName, ctn.getTableName(), null);
		for (SchemaColumnDefinition colName : tableDef.getColumns()) {
			if (first) {
				first = false;
			} else {
				columns.append(',');
			}
			columns.append(colName.getName());
			types.add(colName.getType().getType());
		}
		try {
			select = SelectStatement.parse("SELECT " + columns + " FROM " + vdbTableName + " " + predicateString);
		} catch (ParseException e) {
			throw new RGMAPermanentException("Invalid predicate " + e.getMessage());
		}
		t = new SecondaryProducerTable(super.declareTable(context, ctn, predicateString, hrpSecs, true), select, ctn.getVdbName());
		t.m_insertStringPrefix = "INSERT INTO " + vdbTableName + "(" + columns + ") VALUES (";
		t.m_types = types;
		Task task = new GetPlansTask(t);
		synchronized (m_tables) {
			m_tables.put(t.m_vdbTableName, t);
		}
		s_taskInvocationQueue.add(task);
		registerTable(t, 0); // Initial time is zero
	}

	/**
	 * @return A detailed summary of this particular resource.
	 * @throws RGMAPermanentException
	 */
	String getDetails(boolean tableDetails, boolean queryDetails, boolean fullDetails) throws RGMAPermanentException, RGMAPermanentException {
		ProducerProperties producerProp = getProducerProperties();
		StringBuilder queryType = new StringBuilder();
		queryType.append('[' + producerProp.getProducerType() + ']');
		String storageType = "MEM";
		if (producerProp.getStorage().isDatabase()) {
			storageType = "DB";
		}
		StringBuilder b = new StringBuilder();
		b.append("<Resource ID=\"");
		b.append(getEndpoint().getResourceID());
		b.append("\" ClientHostName=\"");
		b.append(getClientHostName());
		b.append("\" QueryTypes=\"");
		b.append(queryType);
		b.append("\" StorageType=\"");
		b.append(storageType);
		b.append("\" UserLastContactIntervalMillis=\"");
		b.append(getLastContactIntervalMillis());
		b.append("\" LastRegistryUpdateIntervalMillis=\"");
		b.append(getLastRegistryUpdateMillis());
		b.append("\" TerminationIntervalMillis=\"");
		b.append(getTerminationInterval() * 1000);
		b.append("\" ResourceCreationTimeMillis=\"");
		b.append(getTimeCreated());
		synchronized (m_tables) {
			b.append("\" TableCount=\"").append(m_tables.size());
		}
		b.append("\" Status=\"").append(displayStatus());
		b.append("\">\n");

		if (tableDetails) {
			synchronized (m_tables) {
				for (Table t : m_tables.values()) {
					SecondaryProducerTable spt = (SecondaryProducerTable) t;
					synchronized (t) {
						b.append("<Table Name=\"");
						b.append(t.m_vdbTableName);
						b.append("\" HistoryRetentionPeriodMillis=\"");
						b.append(t.m_hrpSecs * 1000);
						b.append("\" LastRegistryUpdateIntervalMillis=\"");
						b.append((t.m_lastRegistryUpdate == 0 ? "Not registered yet" : System.currentTimeMillis() - t.m_lastRegistryUpdate));
						b.append("\" LastSuccessfulInsertIntervalMillis=\"");
						b.append((t.m_lastInsertTime == 0 ? "Awaiting first Insert" : System.currentTimeMillis() - t.m_lastInsertTime));
						b.append("\" TotalNumberInsertedTuples=\"");
						b.append(t.m_totalInsertedTuples);
						b.append("\" TotalNumberOfTuplesInStore=\"");
						b.append(getTotalNoTuplesInStore(t.m_vdbTableName));
						b.append("\" TotalNumberRejectedTuples=\"");
						b.append(t.m_totalRejectedTuples);
						b.append("\" LastRejectedTupleIntervalMillis=\"");
						b.append((t.m_lastRejectedTime == 0 ? "None rejected" : System.currentTimeMillis() - t.m_lastRejectedTime));
						b.append("\" LastRejectedTupleException=\"");
						b.append(t.m_lastRejectedExceptionMsg);
						b.append("\" ConsumerCount=\"");
						b.append(t.m_queries.size());
						b.append("\" ProducerCount=\"");
						b.append(getCountConnectedProducers((SecondaryProducerTable) t));
						b.append("\" Predicate=\"");
						b.append(t.m_predicate.toString().length() > 0);
						b.append("\">\n");

						if (queryDetails) {
							Plan plan;
							synchronized (t) {
								plan = spt.m_plan;
							}
							if (plan != null) {
								synchronized (plan) {
									for (PlanEntry pe : plan.getPlanEntries()) {
										ResourceEndpoint ep = pe.getProducer().getEndpoint();
										b.append("<Producer ID=\"").append(ep.getResourceID()).append("\" ");
										b.append("URL=\"").append(ep.getURL()).append("\" />\n");
									}
								}
							}
							synchronized (t.m_queries) {
								for (Entry<ResourceEndpoint, Query> entry : t.m_queries.entrySet()) {
									ResourceEndpoint consumer = entry.getKey();
									Query query = entry.getValue();
									b.append("<RunningQuery ");
									b.append("ConsumerID=\"");
									b.append(consumer.getResourceID());
									b.append("\" ConsumerURL=\"");
									b.append(consumer.getURL());
									b.append("\" ConsumerPort=\"");
									b.append(query.m_streamingPort);
									b.append("\" Predicate=\"");
									b.append(query.m_hasPredicate);
									b.append("\"/>\n");
								}
							}
						}
						b.append("</Table>");
					}
				}
			}
		} else {
			String tableName = "";
			synchronized (m_tables) {
				for (Table table : m_tables.values()) {
					tableName = table.m_vdbTableName;
					break;
				}
			}
			b.append("<Table ");
			b.append("Name=\"" + tableName + "\"");
			b.append(">\n");
			b.append("</Table>");
		}
		if (fullDetails) {
			b.append(getTasksDisplay());
		}
		b.append("</Resource>\n");
		return b.toString();
	}

	/**
	 * Execute the query plan
	 */
	private void executePlan(SecondaryProducerTable table) {
		Plan plan = table.m_plan;
		synchronized (plan) {
			if (plan.getPlanEntries().size() == 0) {
				if (m_logger.isDebugEnabled()) {
					m_logger.debug("No Entries in plan for secondary producers: " + m_endpoint);
				}
			}
			for (PlanEntry entry : plan.getPlanEntries()) {
				if (m_logger.isDebugEnabled()) {
					m_logger.debug("Executing plan entry: " + entry);
				}
				executePlanEntry(entry, table.m_startTimeMillis);
			}
		}
	}

	/**
	 * Execute a PlanEntry.
	 */
	private void executePlanEntry(PlanEntry entry, long startTime) {

		/* Set the continuous query time interval */
		long queryMillisElapsed = System.currentTimeMillis() - startTime;
		QueryProperties props;
		if (queryMillisElapsed > 0) {
			props = QueryProperties.getContinuous(new TimeInterval(queryMillisElapsed, Units.MILLIS));
		} else {
			props = QueryProperties.CONTINUOUS;
		}

		/* Create a NormalReply and deal with it */
		RunningReply reply = new RunningReply(entry, this, s_taskInvocationQueue, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount, s_intervalToGiveUpOnUnreachableMillis, entry.getSelect()
				.getFrom().get(0).getTable().getVdbTableName());
		s_streamingReceiver.addReply(reply);

		/* This block is to ensure that if a reply on the queue is inactive then it has completed */
		synchronized (m_replies) {
			m_replies.put(entry, reply);
			reply.start(null, props, s_streamingProps, m_context);
		}

		/*
		 * Register the callback method "update" on consumer resource with the producer reply "reply"
		 */
		reply.addObserver(this);

		/* Check the producer periodically */
		synchronized (m_status) {
			if (m_status != Status.DESTROYED) {
				m_Timer.schedule(new ProducerTestTask(reply), s_pingInterval, s_pingInterval);
			}
		}
	}

	private int getCountConnectedProducers(SecondaryProducerTable t) {
		Plan plan = t.m_plan;
		if (plan == null) {
			return 0;
		}
		synchronized (plan) {
			return plan.getPlanEntries().size();
		}
	}

	/**
	 * Modify the query plan. Synchronized on plan to prevent two calls modifying the plan simultaneously
	 */
	private synchronized void modifyPlan(List<PlanInstruction> instructions, Plan plan, long startTime) {
		synchronized (plan) {
			for (PlanInstruction instruction : instructions) {
				if (instruction.getType() == PlanInstruction.Type.ADD) {
					ProducerDetails producer = instruction.getProducer();
					SelectStatement select = instruction.getSelect();
					PlanEntry entry = new PlanEntry(producer, select);
					List<PlanEntry> entries = plan.getPlanEntries();
					/* Prevent addition of a previously added entry */
					if (!entries.contains(entry)) {
						plan.getPlanEntries().add(entry);
						if (instruction.hasWarning()) {
							plan.setWarning(instruction.getWarning());
						}
						executePlanEntry(entry, startTime);
					}
				} else if (instruction.getType() == PlanInstruction.Type.REMOVE) {
					if (instruction.hasWarning()) {
						plan.setWarning(instruction.getWarning());
					}
					Iterator<PlanEntry> peIter = plan.getPlanEntries().iterator();
					while (peIter.hasNext()) { // Need iterator because of the remove from the list
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

	/** Adds a few properties to the basic Table */
	public class SecondaryProducerTable extends Table {
		/**
		 * A list of types of the columns. This is not marked final as it is easiest to build it after the object has
		 * been instantiated. However it is never modified.
		 */
		public List<Type> m_types;

		/**
		 * This is not marked final as it is easiest to build it after the object has been instantiated. However it is
		 * never modified.
		 */
		private String m_insertStringPrefix;

		/** The current plan for this table. It is null until a GetPlansTask has been successful */
		private Plan m_plan;

		/** The task for get or refresh plans */
		private Task m_planTask;

		/** The select statement to get data from the contributing producers */
		private final SelectStatement m_select;

		private final String m_vdbName;

		private SecondaryProducerTable(SecondaryProducerTable spt) {
			super(spt);
			m_insertStringPrefix = spt.m_insertStringPrefix;
			m_plan = spt.m_plan;
			m_planTask = spt.m_planTask;
			m_select = spt.m_select;
			m_types = spt.m_types;
			m_vdbName = spt.m_vdbName;
		}

		private SecondaryProducerTable(Table table, SelectStatement select, String vdbName) {
			super(table);
			m_select = select;
			m_vdbName = vdbName;
		}
	}

	/**
	 * Task which closes the plan and unregisters the consumer. Calls closePlan on the mediator, which in turn calls
	 * unregisterContinuousConsumer on a registry replica.
	 */
	private class ClosePlanTask extends Task {

		private Plan m_plan;

		private String m_vdbName;

		public ClosePlanTask(Plan plan, String vdbName) {
			super(m_endpoint.toString(), "Registryfor:" + vdbName, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_plan = plan;
			m_vdbName = vdbName;
		}

		@Override
		public Result invoke() {
			synchronized (SecondaryProducerResource.this) {
				Result result = Result.SUCCESS;
				Set<String> vdbNames = new HashSet<String>();
				vdbNames.add(m_vdbName);
				try {
					s_mediator.closePlan(m_endpoint, vdbNames);
					if (m_logger.isDebugEnabled()) {
						m_logger.debug("Secondary Producer " + m_endpoint.getResourceID() + " closed plan " + m_plan);
					}
				} catch (RGMAPermanentException e) {
					m_logger.error("Unexpected error closing plan", e);
					result = Result.HARD_ERROR;

				} catch (RGMATemporaryException e) {
					m_logger.warn("Could not contact required registry to close plan: " + e.getMessage());
					result = Result.SOFT_ERROR;
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

		private SecondaryProducerTable m_table;

		public GetPlansTask(SecondaryProducerTable table) {
			super(m_endpoint.toString(), "Registryfor:" + table.m_vdbName, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_table = table;
		}

		@Override
		public Result invoke() {

			synchronized (SecondaryProducerResource.this) {
				Result result = Result.SUCCESS;
				try {
					Plan plan;
					synchronized (m_table) {
						plan = m_table.m_plan;
					}
					if (plan == null) {
						List<Plan> plans = s_mediator.getPlansForQuery(m_endpoint, m_table.m_select, QueryProperties.CONTINUOUS, s_registryTerminationInterval,
								s_noTablesForViews, true);
						List<Plan> plansWithoutWarning = new ArrayList<Plan>();
						List<Plan> plansWithWarning = new ArrayList<Plan>();
						for (Plan p : plans) {
							if (!p.hasWarning()) {
								plansWithoutWarning.add(p);
							} else {
								plansWithWarning.add(p);
							}
						}
						if (m_logger.isDebugEnabled()) {
							m_logger.debug("Secondary Producer " + m_endpoint.getResourceID() + " got " + plans.size() + " plans, " + plansWithWarning.size()
									+ " with warning, " + plansWithoutWarning.size() + " without");
						}
						if (plansWithoutWarning.size() > 0) {
							plan = chooseRandomPlan(plansWithoutWarning);
						} else {
							plan = chooseRandomPlan(plansWithWarning);
						}
						synchronized (m_table) {
							m_table.m_plan = plan;
						}
						executePlan(m_table);
					}
				} catch (RGMAPermanentException e) {
					m_logger.error("Unexpected error getting plans from mediator", e);
					result = Result.HARD_ERROR;
				} catch (RGMATemporaryException e) {
					m_logger.warn("Could not contact required registry to get plan from mediator: " + e.getMessage());
					result = Result.SOFT_ERROR;
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
	 * Task which refreshes the Secondary Producer's query plan. This is implemented as a RemoveInvocation since the
	 * mediator will contact registry replicas to check if any new producers have been added. This Task has a dependency
	 * on the initial GetPlans message.
	 */
	private class RefreshPlanTask extends Task {

		private SecondaryProducerTable m_table;

		public RefreshPlanTask(SecondaryProducerTable table) {
			super(m_endpoint.toString(), "Registryfor:" + table.m_vdbName, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_table = table;
		}

		@Override
		public Result invoke() {
			synchronized (SecondaryProducerResource.this) {
				Result result = Result.SUCCESS;
				try {
					Plan plan;
					synchronized (m_table) {
						plan = m_table.m_plan;
					}
					if (plan != null) {
						List<PlanInstruction> instructions = s_mediator.refreshPlan(m_endpoint, m_table.m_select, QueryProperties.CONTINUOUS, plan,
								s_registryTerminationInterval, s_noTablesForViews, true);
						modifyPlan(instructions, plan, m_table.m_startTimeMillis);
					}
				} catch (RGMAPermanentException e) {
					m_logger.error("Unexpected error refreshing plan", e);
					result = Result.HARD_ERROR;
				} catch (RGMATemporaryException e) {
					m_logger.warn("Failed to refresh plan: " + e);
					result = Result.SOFT_ERROR;
				}
				return result;
			}
		}
	}

	/**
	 * Task which removes a producer from the Secondary Producer's query plan. This is implemented as a RemoveInvocation
	 * since the mediator may contact registry replicas to patch the query plan (although this will not normally be
	 * necessary). This Task has a dependency on the previous add/remove producer request for this producer endpoint, or
	 * if no previous request exists, on the initial GetPlans message. A producer may provide data for more than one
	 * table - so try them all
	 */
	private class RemoveProducerTask extends Task {

		private final ResourceEndpoint m_producer;

		private final SecondaryProducerTable m_table;

		public RemoveProducerTask(ResourceEndpoint producer, SecondaryProducerTable table) {
			super(m_endpoint.toString(), "Registryfor:" + table.m_vdbName, s_maximumTaskTimeMillis, s_maximumTaskAttemptCount);
			m_producer = producer;
			m_table = table;
		}

		@Override
		public Result invoke() {
			synchronized (SecondaryProducerResource.this) {
				Result result = Result.SUCCESS;
				try {
					Plan plan;
					synchronized (m_table) {
						plan = m_table.m_plan;
					}
					if (plan != null) {
						List<PlanInstruction> instructions = s_mediator.removeProducerFromPlan(m_endpoint, m_table.m_select, QueryProperties.CONTINUOUS, plan,
								m_producer, s_noTablesForViews, true);
						modifyPlan(instructions, plan, m_table.m_startTimeMillis);
						if (m_logger.isDebugEnabled()) {
							m_logger.debug("Secondary Producer " + m_endpoint + " removed from plan for " + m_table.m_vdbTableName + " to give " + plan);
						}
					}
				} catch (RGMAPermanentException e) {
					m_logger.error("Unexpected error removing producer " + e);
					result = Result.HARD_ERROR;

				} catch (RGMATemporaryException e) {
					m_logger.warn("Failed to get new plan on removeProducer: " + e);
					result = Result.SOFT_ERROR;
				}
				return result;
			}
		}
	}
}