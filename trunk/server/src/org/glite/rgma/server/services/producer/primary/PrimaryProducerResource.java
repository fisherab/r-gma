/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.producer.primary;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.database.SQLTypeAdjuster;
import org.glite.rgma.server.services.producer.ProducerResource;
import org.glite.rgma.server.services.producer.RunningQuery;
import org.glite.rgma.server.services.producer.store.ReservedColumns;
import org.glite.rgma.server.services.producer.store.TupleStore.BufferFullException;
import org.glite.rgma.server.services.schema.Authz;
import org.glite.rgma.server.services.sql.Constant;
import org.glite.rgma.server.services.sql.Expression;
import org.glite.rgma.server.services.sql.InsertStatement;
import org.glite.rgma.server.services.sql.SQLExpEvaluator;
import org.glite.rgma.server.services.sql.TableName;
import org.glite.rgma.server.services.sql.Tuple;
import org.glite.rgma.server.services.sql.WhereClause;
import org.glite.rgma.server.services.sql.Constant.Type;
import org.glite.rgma.server.services.sql.SQLExpEvaluator.NullFound;
import org.glite.rgma.server.services.sql.Tuple.UnknownAttribute;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.ProducerProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.SchemaColumnDefinition;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.UserContext;

public class PrimaryProducerResource extends ProducerResource {

	private static final Set<String> s_MetadataColumns = new HashSet<String>();

	private static final Set<String> s_readonlyMetadataColumns = new HashSet<String>();

	private static int s_cleanupIntervalMillis;

	private long m_timeToDestroyMillis;

	static {
		final Set<String> s_writeableMetadataColumns = new HashSet<String>();
		s_readonlyMetadataColumns.add(ReservedColumns.RGMA_LRT_COLUMN_NAME);
		s_readonlyMetadataColumns.add(ReservedColumns.RGMA_ORIGINAL_SERVER);
		s_readonlyMetadataColumns.add(ReservedColumns.RGMA_ORIGINAL_CLIENT);
		s_writeableMetadataColumns.add(ReservedColumns.RGMA_TIMESTAMP_COLUMN_NAME);
		s_MetadataColumns.addAll(s_readonlyMetadataColumns);
		s_MetadataColumns.addAll(s_writeableMetadataColumns);
	}

	/**
	 * Constructor of PrimaryProducerResource
	 * 
	 * @param terminationInterval
	 * @param prop
	 * @param endpoint
	 * @param clientHostName
	 * @param creatorDn
	 * @throws ConfigurationException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 */
	public PrimaryProducerResource(UserContext userContext, ProducerProperties properties, ResourceEndpoint endpoint) throws RGMAPermanentException,
			RGMAPermanentException {
		super(endpoint, userContext, properties, Logger.getLogger(PrimaryProducerConstants.PRIMARY_PRODUCER_LOGGER));
		m_status = Status.ACTIVE;
	}

	private class PrimaryProducerTable extends Table {
		private final int m_lrpSecs;

		public PrimaryProducerTable(Table table, int lrpSecs) {
			super(table);
			m_lrpSecs = lrpSecs;
		}
	}

	/**
	 * This method is called once when the PrimaryProducerService starts up to initialize static variables
	 * 
	 * @param service
	 * @throws ConfigurationException
	 * @throws RGMAPermanentException
	 */
	protected static void setStaticVariables(PrimaryProducerService service, TimeInterval registryTerminationInterval, String hostname)
			throws RGMAPermanentException {
		ProducerResource.setStaticVariables(registryTerminationInterval, hostname);
		s_cleanupIntervalMillis = ServerConfig.getInstance().getInt(ServerConstants.PRIMARY_PRODUCER_CLEANUP_INTERVAL_SECS) * 1000;
	}

	/**
	 * Adds a table to the list of tables to which this producer may publish tuples.
	 * 
	 * @throws RGMANoWorkingReplicasException
	 */
	public void declareTable(UserContext context, String userTableName, String predicate, int hrpSecs, int lrpSecs) throws RGMAPermanentException,
			RGMAPermanentException, RGMAPermanentException {
		checkContext(context);
		TableName ctn = new TableName(userTableName);
		PrimaryProducerTable t;
		synchronized (m_tables) {
			t = (PrimaryProducerTable) m_tables.get(ctn.getVdbTableName());
		}
		if (t != null) {
			if (t.m_hrpSecs != hrpSecs) {
				throw new RGMAPermanentException("Table " + userTableName + " has already been declared with different HRP value");
			}
			if (t.m_lrpSecs != lrpSecs) {
				throw new RGMAPermanentException("Table " + userTableName + " has already been declared with different LRP value");
			}
			return;
		}
		if (lrpSecs <= 0) {
			throw new RGMAPermanentException("LRP must be > 0 and not " + lrpSecs);
		}
		t = new PrimaryProducerTable(super.declareTable(context, ctn, predicate, hrpSecs, false), lrpSecs);
		synchronized (m_tables) {
			m_tables.put(t.m_vdbTableName, t);
		}
		registerTable(t, 0); // Initial time is zero
	}

	/**
	 * Inserts a tuple into the tuple store for the specified primary producer resource.
	 */
	public void insert(UserContext context, String insertString, int lrpSec) throws RGMAPermanentException, RGMAPermanentException, RGMAPermanentException,
			RGMATemporaryException {
		long now = System.currentTimeMillis();
		String date = new Date(now).toString();
		String time = new Time(now).toString();
		insert(context, insertString, 0, 1, lrpSec, now, date, time);
	}

	private static Tuple buildTupleFromInsert(InsertStatement insertStatement) throws RGMAPermanentException {
		Iterator<Constant> valuesIterator = insertStatement.getColumnValues().iterator();
		Iterator<String> columnsIterator = insertStatement.getColumnNames().iterator();

		Tuple tuple = new Tuple();
		while (valuesIterator.hasNext()) {
			Constant valConst = valuesIterator.next();
			Object val;
			if (valConst.getType() == Constant.Type.NULL) {
				val = null;
			} else if (valConst.getType() == Constant.Type.NUMBER) {
				try {
					val = new Integer(valConst.getValue());
				} catch (NumberFormatException e1) {
					try {
						val = new Double(valConst.getValue());
					} catch (NumberFormatException e2) {
						throw new RGMAPermanentException("Unexpected value in tuple: " + valConst.getValue());
					}
				}
			} else { // STRING
				val = valConst.getValue();
			}
			tuple.addAttribute(columnsIterator.next(), val);
		}
		return tuple;
	}

	private void insert(UserContext context, String insertString, int n, int ntotal, int lrpSec, long now, String date, String time)
			throws RGMAPermanentException, RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		InsertStatement insertStmt = null;
		PrimaryProducerTable table = null;
		try {
			insertStmt = InsertStatement.parse(insertString);
			String name = insertStmt.getTableName().getVdbTableName();

			synchronized (m_tables) {
				table = (PrimaryProducerTable) m_tables.get(name);
			}
			if (table == null) {
				throw new RGMAPermanentException("Table '" + name + "' has not been declared");
			}
			if (n == 0) {
				checkContext(context);
			}

			SQLExpEvaluator exp = new SQLExpEvaluator();
			buildInsertStatement(table, insertStmt, lrpSec, now, date, time, context);

			/* Build tuple as key value pairs */
			Tuple insertTuple = buildTupleFromInsert(insertStmt);

			/* Check producer predicate */
			String predicate = table.m_predicate.toString();

			if (predicate.length() != 0) {
				try {
					if (!exp.eval(insertTuple, WhereClause.parse(predicate).getExpression())) {
						throw new RGMAPermanentException("Tuple is not consistent with producer predicate.");
					}
				} catch (NullFound e) {
					throw new RGMAPermanentException("Predicate may not contain IS NULL nor IS NOT NULL.");
				} catch (UnknownAttribute e) {
					throw new RGMAPermanentException("Tuple is not consistent with producer predicate.");
				}
			}

			/*
			 * Check against write authorization rules - the list of table names is not required
			 */
			Expression authPredicate = Authz.constructAuthPredicate(null, context.getDN(), context.getFQANs(), table.m_authz,Authz.RuleType.DATA,  'W');
			try {
				if (!exp.eval(insertTuple, authPredicate)) {
					throw new RGMAPermanentException("Tuple is not compatible with write rules for this table.");
				}
			} catch (NullFound e) {
				throw new RGMAPermanentException(e);
			} catch (UnknownAttribute e) {
				throw new RGMAPermanentException("Unknown attribute noted when checking authz rules");
			}

			m_tupleStore.insert(context, insertStmt);
			synchronized (table) {
				table.m_lastInsertTime = System.currentTimeMillis();
				table.m_totalInsertedTuples++;
			}
		} catch (RGMAPermanentException e) {
			e.setNumSuccessfulOps(n);
			logReject(table, e);
			throw e;
		} catch (BufferFullException e) {
			RGMATemporaryException re = new RGMATemporaryException(e.getMessage());
			re.setNumSuccessfulOps(n);
			logReject(table, re);
			throw re;
		} catch (ParseException e) {
			RGMAPermanentException re = new RGMAPermanentException("Parsing error: " + e.getMessage());
			re.setNumSuccessfulOps(n);
			logReject(table, re);
			throw re;
		} catch (SQLException e) {
			RGMAPermanentException re = new RGMAPermanentException(e);
			re.setNumSuccessfulOps(n);
			logReject(table, re);
			throw re;
		}
	}

	private void badType(String col, String value, Type actualValueType, String type) throws RGMAPermanentException {
		if (actualValueType == Type.STRING) {
			value = "'" + value + "'";
		}
		throw new RGMAPermanentException("Column  '" + col + "' is of type '" + type + "' and is not compatible with " + actualValueType + " value: " + value);
	}

	/**
	 * Build Insert Statement by adding metadatacolumns. Upon return the insert statement will be correct and will have
	 * a list of column names and corrsponding values.
	 * 
	 * @param insertStatement
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 */
	private void buildInsertStatement(PrimaryProducerTable table, InsertStatement insertStmt, int lrpsec, long now, String dateString, String timeString,
			UserContext context) throws RGMAPermanentException, RGMAPermanentException {
		/* First check all the column values are good - and watch for writeable metadata */
		if (lrpsec == 0) {
			lrpsec = table.m_lrpSecs;
		}
		SchemaTableDefinition td = table.m_def;
		List<Constant> insertStmtColumnValues = insertStmt.getColumnValues();
		List<String> insertStatementColumnNames = insertStmt.getColumnNames();

		Iterator<String> actualColumnNamesIterator = insertStatementColumnNames.iterator();
		List<SchemaColumnDefinition> schemaColumns = td.getColumns();
		String rgmaTimestamp = null;
		Set<String> columnsNamesFound = new HashSet<String>();
		for (Constant valConst : insertStmtColumnValues) {
			String col = actualColumnNamesIterator.next();
			boolean found = false;
			for (SchemaColumnDefinition cd : schemaColumns) {
				String schemaColumnName = cd.getName();
				if (col.equalsIgnoreCase(schemaColumnName)) {
					if (columnsNamesFound.contains(schemaColumnName)) {
						throw new RGMAPermanentException("Column name '" + schemaColumnName + "' appears more than once.");
					}
					found = true;
					columnsNamesFound.add(schemaColumnName);
					String value = valConst.getValue();
					Type actualValueType = valConst.getType();
					org.glite.rgma.server.services.sql.DataType.Type baseType = cd.getType().getType();
					try {
						if (s_readonlyMetadataColumns.contains(schemaColumnName)) {
							throw new RGMAPermanentException("Column '" + schemaColumnName + "' is read only.");
						}
						if (actualValueType == Type.NULL) {
							if (cd.isNotNull()) {
								throw new RGMAPermanentException("Column '" + col + "' must not be null.");
							}
						} else if (baseType == org.glite.rgma.server.services.sql.DataType.Type.INTEGER) {
							if (actualValueType != Type.NUMBER) {
								badType(col, value, actualValueType, cd.getType().toString());
							}
							valConst.setValue(SQLTypeAdjuster.checkInteger(value));
						} else if (baseType == org.glite.rgma.server.services.sql.DataType.Type.REAL) {
							if (actualValueType != Type.NUMBER) {
								badType(col, value, actualValueType, cd.getType().toString());
							}
							Float.parseFloat(value);
						} else if (baseType == org.glite.rgma.server.services.sql.DataType.Type.DOUBLE_PRECISION) {
							if (actualValueType != Type.NUMBER) {
								badType(col, value, actualValueType, cd.getType().toString());
							}
							Double.parseDouble(value);
						} else if (baseType == org.glite.rgma.server.services.sql.DataType.Type.DATE) {
							if (actualValueType != Type.STRING) {
								badType(col, value, actualValueType, cd.getType().toString());
							}
							valConst.setValue(SQLTypeAdjuster.checkDate(value));
						} else if (baseType == org.glite.rgma.server.services.sql.DataType.Type.TIME) {
							if (actualValueType != Type.STRING) {
								badType(col, value, actualValueType, cd.getType().toString());
							}
							valConst.setValue(SQLTypeAdjuster.checkTime(value));
						} else if (baseType == org.glite.rgma.server.services.sql.DataType.Type.TIMESTAMP) {
							if (actualValueType != Type.STRING) {
								badType(col, value, actualValueType, cd.getType().toString());
							}
							valConst.setValue(SQLTypeAdjuster.checkTimestamp(value));
						} else if (baseType == org.glite.rgma.server.services.sql.DataType.Type.CHAR
								|| baseType == org.glite.rgma.server.services.sql.DataType.Type.VARCHAR) {
							if (actualValueType != Type.STRING) {
								badType(col, value, actualValueType, cd.getType().toString());
							}
							if (value.length() > cd.getType().getSize()) {
								badType(col, value, actualValueType, cd.getType().toString());
							}
						}
					} catch (NumberFormatException e) {
						badType(col, value, actualValueType, cd.getType().toString());
					} catch (RGMAPermanentException e) {
						/* This is thrown by the DateTime checking methods */
						throw new RGMAPermanentException(e.getMessage());
					}
					if (col.equalsIgnoreCase(ReservedColumns.RGMA_TIMESTAMP_COLUMN_NAME)) {
						rgmaTimestamp = valConst.getValue();
					}
					break;
				}
			}
			if (!found) {
				throw new RGMAPermanentException("Column '" + col + "' is not defined in this table.");
			}
		}
		for (SchemaColumnDefinition cd : schemaColumns) {
			String cname = cd.getName();
			if (cd.isNotNull() && !columnsNamesFound.contains(cname)) {
				if (!s_MetadataColumns.contains(cname)) {
					throw new RGMAPermanentException("A value for column '" + cd.getName() + "' must be specified as it has the NOT NULL characteristic.");
				}
			}
		}
		/* Set the metadata columns */
		String datetimeString;
		String lrtString;
		if (rgmaTimestamp == null) {
			datetimeString = dateString + " " + timeString;
			lrtString = new Timestamp(now + lrpsec * 1000).toString();
		} else {
			datetimeString = rgmaTimestamp;
			lrtString = new Timestamp(Timestamp.valueOf(datetimeString).getTime() + lrpsec * 1000).toString();
		}
		if (rgmaTimestamp == null) {
			insertStatementColumnNames.add(ReservedColumns.RGMA_TIMESTAMP_COLUMN_NAME);
			insertStmtColumnValues.add(new Constant(datetimeString, Constant.Type.STRING));
		}
		insertStatementColumnNames.add(ReservedColumns.RGMA_LRT_COLUMN_NAME);
		insertStmtColumnValues.add(new Constant(lrtString, Constant.Type.STRING));
		insertStatementColumnNames.add(ReservedColumns.RGMA_ORIGINAL_SERVER);
		insertStmtColumnValues.add(new Constant(s_hostname, Constant.Type.STRING));
		insertStatementColumnNames.add(ReservedColumns.RGMA_ORIGINAL_CLIENT);
		insertStmtColumnValues.add(new Constant(context.getHostName(), Constant.Type.STRING));
	}

	/**
	 * Inserts a number of tuples into the tuple store for the specified primary producer resource.
	 * 
	 * @throws RGMATemporaryException
	 */
	public void insertList(UserContext userContext, List<String> insertStrings, int lrpSec) throws RGMAPermanentException, RGMAPermanentException,
			RGMAPermanentException, RGMATemporaryException {
		long now = System.currentTimeMillis();
		String date = new java.sql.Date(now).toString();
		String time = new java.sql.Time(now).toString();
		int count = 0;
		for (String insertString : insertStrings) {
			insert(userContext, insertString, count, insertStrings.size(), lrpSec, now, date, time);
			count++;
		}
	}

	/**
	 * Gets the LatestRetentionPeriod.
	 * 
	 * @return LatestRetentionPeriod The minimum time for which latest tuples are stored.
	 * @throws RGMAException
	 *             Thrown if not connected.
	 */
	public int getLatestRetentionPeriod(String tableName) throws RGMAPermanentException {
		TableName ctn = new TableName(tableName);
		if (ctn.getVdbName() == null) {
			throw new RGMAPermanentException("Table name must include the vdb prefix");
		}
		String vdbTableName = ctn.getVdbTableName();
		synchronized (m_tables) {
			PrimaryProducerTable t = (PrimaryProducerTable) m_tables.get(vdbTableName);
			if (t == null) {
				throw new RGMAPermanentException("Table not declared: " + vdbTableName);
			}
			return t.m_lrpSecs;
		}
	}

	/**
	 * Closed primary producers can be destroyed when the HRP is exceeded for those with memory storage and when one
	 * attempt has been to stream all tuples to existing consumers.
	 * 
	 * @throws RGMAPermanentException
	 */
	@Override
	public boolean canDestroy() throws RGMAPermanentException {
		/* Set up timeout */
		long now = System.currentTimeMillis();
		if (m_timeToDestroyMillis == 0) {
			m_timeToDestroyMillis = now + s_cleanupIntervalMillis;
		}
		boolean timedOut = now > m_timeToDestroyMillis;

		List<Table> tables = null;
		synchronized (m_tables) {
			tables = new ArrayList<Table>(m_tables.values());
		}
		for (Table table : tables) {
			String vdbTableName = table.m_vdbTableName;
			/* Honour the HRP for memory based storage */
			if (m_properties.getStorage().isMemory()) {
				if (m_tupleStore.getHistoryCount(vdbTableName) > 0) {
					m_tupleStore.cleanUpHRP(vdbTableName);
					long count = m_tupleStore.getHistoryCount(vdbTableName);
					if (count > 0) {
						if (m_logger.isDebugEnabled()) {
							m_logger.debug("HRP count for table :" + vdbTableName + " is " + count + " for ProducerID :" + m_endpoint.getResourceID()
									+ " and cannot be destroyed");
						}
						return false;
					}
				}
			}
			/*
			 * Ensure that tuples are streamed to all existing continuous consumers. Unless the timeout has expired this
			 * is done by checking that for each table the set of consumers has been set up by consulting the registry,
			 * all the connections have been made, and all tuples have been streamed.
			 */
			if (timedOut) {
				continue;
			}

			synchronized (table) {
				if (table.m_lastRegistryUpdate == 0) {
					if (m_logger.isDebugEnabled()) {
						m_logger.debug(m_endpoint + " " + table.m_vdbTableName + " is not yet registered so cannot be destroyed");
					}
					return false;
				}
			}

			synchronized (table.m_continuousConsumers) {
				for (ContinuousConsumer consumer : table.m_continuousConsumers.values()) {
					if (!consumer.m_connected) {
						if (m_logger.isDebugEnabled()) {
							m_logger.debug("Consumer " + consumer.m_consumerEp + " of " + vdbTableName + " is not yet connected for ProducerID "
									+ m_endpoint.getResourceID() + " and cannot be destroyed");
						}
						return false;
					}
				}
			}
			synchronized (table.m_queries) {
				for (Query query : table.m_queries.values()) {
					if (query.m_isContinuous) {
						RunningQuery r = query.m_runningQuery;
						if (r.moreDateToMove()) {
							if (m_logger.isDebugEnabled()) {
								m_logger.debug(r + " has not yet dealt with all tuples for " + vdbTableName + ". Producer resource cannot be destroyed");
							}
							return false;
						}
					}
				}
			}
		}
		synchronized (m_status) {
			if (m_status == Status.CLOSED) {
				if (m_logger.isDebugEnabled()) {
					if (timedOut) {
						m_logger.debug("Producer " + m_endpoint.getResourceID() + " has timed out (it may have completed properly) and can now be destroyed");
					} else {
						m_logger.debug("Producer " + m_endpoint.getResourceID() + " can now be destroyed");
					}
				}
			}
			return m_status == Status.CLOSED;
		}
	}

	/**
	 * Destroy the producer resource. Synchronized on instance since it involves a state change.
	 * 
	 * @throws RGMAPermanentException
	 */
	@Override
	public synchronized void destroy() throws RGMAPermanentException {
		synchronized (m_status) {
			if (m_status == Status.DESTROYED) {
				return;
			}
		}
		try {
			super.destroy();
		} finally {
			synchronized (m_status) {
				m_status = Status.DESTROYED;
			}
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Destroyed primary producer resource " + m_endpoint.getResourceID());
		}
	}

	@Override
	public void updateRegistry() {
		try {
			synchronized (this) {
				synchronized (m_status) {
					if (m_status == Status.DESTROYED) {
						return;
					}
				}
				synchronized (m_tables) {
					for (Table t : m_tables.values()) {
						int hrpSecs = Math.min(t.m_hrpSecs, (int) ((System.currentTimeMillis() - t.m_startTimeMillis) / 1000L));
						registerTable(t, hrpSecs);
						if (m_logger.isInfoEnabled()) {
							m_logger.info("Queued update registry for PrimaryResource ID :" + m_endpoint.getResourceID() + " for Table :" + t.m_vdbTableName);
						}
					}
				}
			}
		} catch (RGMAPermanentException e) {
			m_logger.warn("registerTable failed with internal exception " + e.getFlattenedMessage());
		}
	}

	/**
	 * @return A detailed summary of this particular resource.
	 * @throws RGMAPermanentException
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
					PrimaryProducerTable ppt = (PrimaryProducerTable) t;
					synchronized (t) {
						b.append("<Table Name=\"");
						b.append(t.m_vdbTableName);
						b.append("\" HistoryRetentionPeriodMillis=\"");
						b.append(t.m_hrpSecs * 1000);
						b.append("\" LatestRetentionPeriodMillis=\"");
						b.append(ppt.m_lrpSecs * 1000);
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
						b.append("\" Predicate=\"");
						b.append(t.m_predicate.toString().length() > 0);
						b.append("\">\n");

						if (queryDetails) {
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
}