/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.schema.Authz;
import org.glite.rgma.server.services.sql.ColumnDefinition;
import org.glite.rgma.server.services.sql.Constant;
import org.glite.rgma.server.services.sql.CreateIndexStatement;
import org.glite.rgma.server.services.sql.CreateTableStatement;
import org.glite.rgma.server.services.sql.Expression;
import org.glite.rgma.server.services.sql.ExpressionOrConstant;
import org.glite.rgma.server.services.sql.GroupByHaving;
import org.glite.rgma.server.services.sql.InsertStatement;
import org.glite.rgma.server.services.sql.OrderBy;
import org.glite.rgma.server.services.sql.SelectItem;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.TableName;
import org.glite.rgma.server.services.sql.TableNameAndAlias;
import org.glite.rgma.server.services.sql.TableReference;
import org.glite.rgma.server.services.sql.UpdateStatement;
import org.glite.rgma.server.services.sql.WhereClause;
import org.glite.rgma.server.services.sql.Constant.Type;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.streaming.StreamingSender;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.TupleSetWithLastTUID;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserSystemContext;

/**
 * Stores tuples for primary and secondary producers. Can be temporary (for the lifetime of the producer) or permanent.
 * Has a logical name, specified by the user (permanent) or the system (temporary). For each table created by the user,
 * two tables are created in the tuple store, one for latest and one for history/continuous tuples. Private metadata:
 * <b>RgmaTUID</b> a tuple sequence number (unique to the table in this store, for the lifetime of the store)
 * <b>RgmaInsertTime</b> the system time when the tuple was inserted (Java System.currentTimeMillis()) then insert() a
 * tuple for any given primary key).
 */
public class TupleStore {
	@SuppressWarnings("serial")
	public class BufferFullException extends Exception {
		private BufferFullException(String message) {
			super(message);
		}
	}

	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger(TupleStoreConstants.TUPLE_STORE_LOGGER);

	/**
	 * Creates a LATEST update statement from the given insert.
	 * 
	 * @param insert
	 *            SQL INSERT statement.
	 * @return An SQL UPDATE statement.
	 */
	private static UpdateStatement createLatestUpdate(List<ColumnDefinition> columns, InsertStatement insert) {
		Map<String, Constant> insertMap = null;

		// Set up map
		insertMap = new HashMap<String, Constant>();
		Iterator<Constant> valueIter = insert.getColumnValues().iterator();
		for (String colName : insert.getColumnNames()) {
			insertMap.put(colName.toUpperCase(), valueIter.next());
		}

		Map<String, ExpressionOrConstant> set = new HashMap<String, ExpressionOrConstant>();
		Expression where = null;
		int colNum = 0;

		for (ColumnDefinition def : columns) {
			String colName = def.getName().toUpperCase();
			Constant colValue = insertMap.get(colName);
			if (colValue == null) {
				colValue = new Constant("", Type.NULL);
			}
			if (def.isPrimaryKey()) {
				Expression equalsOp = new Expression("=", new Constant(def.getName(), Constant.Type.COLUMN_NAME), colValue);
				if (where == null) {
					where = equalsOp;
				} else {
					where = new Expression("AND", where, equalsOp);
				}
			} else {
				set.put(def.getName(), colValue);
			}
			colNum++;
		}

		UpdateStatement update = new UpdateStatement(insert.getTableName());
		update.setSet(set);

		Constant newTupleTimestamp = insertMap.get(ReservedColumns.RGMA_TIMESTAMP_COLUMN_NAME.toUpperCase());

		Expression timestampPredicate = new Expression(">=", newTupleTimestamp, new Constant(ReservedColumns.RGMA_TIMESTAMP_COLUMN_NAME,
				Constant.Type.COLUMN_NAME));
		update.setWhere(new Expression("AND", where, timestampPredicate));
		return update;
	}

	private static SelectStatement getLatestSelectStatement(SelectStatement select) throws ParseException {
		List<TableReference> fromTables = select.getFrom();
		String timestampString = new Timestamp(System.currentTimeMillis()).toString();
		/* TODO remove the one table restriction */
		if (fromTables.size() == 1) {
			String aliasTableName = fromTables.get(0).getTable().getVdbTableName();
			String lrtWhere = "WHERE " + aliasTableName + "." + ReservedColumns.RGMA_LRT_COLUMN_NAME + " > " + "'" + timestampString + "'";
			select.addWhere(new Expression("AND", select.getWhere(), WhereClause.parse(lrtWhere).getExpression()));
		}
		return select;
	}

	/**
	 * @param aliases
	 * @param val
	 * @param newValue
	 * @return
	 * @throws DatabaseException
	 */
	private static String makeLatestColumnName(String val, QueryProperties queryProps, Set<String> aliases, Map<String, String> mapping) {
		String newValue = val;
		if (val != null) {
			String[] fullName = val.split("\\.");

			if (fullName.length == 1) { // column
				newValue = val;
			} else if (fullName.length == 2) { // table.column
				if (!aliases.contains(fullName[0])) {
					newValue = new StringBuffer(mapping.get("DEFAULT." + fullName[0].toUpperCase())).append(".").append(fullName[1]).toString();
				} else {
					newValue = new StringBuffer(fullName[0]).append(".").append(fullName[1]).toString();
				}
			} else { // schema.table.column
				if (!aliases.contains(fullName[1])) {
					newValue = new StringBuffer(mapping.get(fullName[0].toUpperCase() + "." + fullName[1].toUpperCase())).append(".").append(fullName[2])
							.toString();
				} else {
					newValue = new StringBuffer(fullName[0]).append(".").append(fullName[1]).append(".").append(fullName[2]).toString();
				}
			}
		}
		return newValue;
	}

	/**
	 * Creates a new ExpSelConst with all references to table names converted to the LATEST version: i.e. tableNameX -->
	 * tableNameX_LATEST. Table names that occur in <code>aliases</code> are not converted.
	 * 
	 * @param expression
	 *            Expression to convert.
	 * @param aliases
	 *            Set of alias names.
	 * @return A new LATEST ExpSelConst.
	 * @throws DatabaseException
	 */
	private static ExpressionOrConstant makeLatestExpSelConst(ExpressionOrConstant expression, QueryProperties queryProps, Set<String> aliases,
			Map<String, String> mapping) {
		if (expression instanceof Expression) {
			Expression exp = (Expression) expression;

			if (exp.nbOperands() == 0) {
				return new Expression(exp.getOperator());
			} else if (exp.nbOperands() == 1) {
				return new Expression(exp.getOperator(), makeLatestExpSelConst(exp.getOperand(0), queryProps, aliases, mapping));
			} else {
				return new Expression(exp.getOperator(), makeLatestExpSelConst(exp.getOperand(0), queryProps, aliases, mapping), makeLatestExpSelConst(exp
						.getOperand(1), queryProps, aliases, mapping));
			}
		} else if (expression instanceof Constant) {
			Constant con = (Constant) expression;
			String val = con.getValue();
			String newValue = val;

			if (con.getType() == Constant.Type.COLUMN_NAME) {
				newValue = makeLatestColumnName(val, queryProps, aliases, mapping);
			}

			return new Constant(newValue, con.getType());
		} else { // Can't happen

			return null;
		}
	}

	/**
	 * Creates a new GroupByHaving object for the LATEST equivalent of the given query.
	 * 
	 * @param query
	 *            SQL HISTORY query.
	 * @param aliases
	 *            Set of alias names.
	 * @return A new GroupByHaving object for the LATEST equivalent of the given query.
	 * @throws DatabaseException
	 */
	private static GroupByHaving makeLatestGroupBy(SelectStatement query, QueryProperties queryProps, Set<String> aliases, Map<String, String> mapping) {
		if (query.getGroupBy() == null) {
			return null;
		}
		List<ExpressionOrConstant> newGroupByList = new ArrayList<ExpressionOrConstant>();

		for (ExpressionOrConstant groupBy : query.getGroupBy().getGroupBy()) {
			newGroupByList.add(makeLatestExpSelConst(groupBy, queryProps, aliases, mapping));
		}

		GroupByHaving newGroupBy = new GroupByHaving(newGroupByList);

		if (query.getGroupBy() != null && query.getGroupBy().getHaving() != null) {
			newGroupBy.setHaving(makeLatestExpSelConst(query.getGroupBy().getHaving(), queryProps, aliases, mapping));
		}

		return newGroupBy;
	}

	/**
	 * Creates a new OrderBy list for the LATEST equivalent of the given query.
	 * 
	 * @param query
	 *            SQL HISTORY query.
	 * @param aliases
	 *            Set of alias names.
	 * @return A new OrderBy list for the LATEST equivalent of the given query.
	 * @throws DatabaseException
	 */
	private static List<OrderBy> makeLatestOrderBy(SelectStatement query, QueryProperties queryProps, Set<String> aliases, Map<String, String> mapping) {
		if (query.getOrderBy() == null) {
			return null;
		}
		List<OrderBy> newOrderBy = new ArrayList<OrderBy>();

		if (query.getOrderBy() != null) {
			for (OrderBy orderBy : query.getOrderBy()) {
				OrderBy ob = new OrderBy(makeLatestExpSelConst(orderBy.getExpression(), queryProps, aliases, mapping));
				ob.setAscOrder(orderBy.getAscOrder());
				newOrderBy.add(ob);
			}
		}

		return newOrderBy;
	}

	/**
	 * Creates a new SelectItem list for the LATEST equivalent of the given query.
	 * 
	 * @param query
	 *            SQL HISTORY query.
	 * @param aliases
	 *            Set of alias names.
	 * @return A new SelectItem list for the LATEST equivalent of the given query.
	 * @throws DatabaseException
	 */
	private static List<SelectItem> makeLatestSelect(SelectStatement query, QueryProperties queryProps, Set<String> aliases, Map<String, String> mapping) {
		List<SelectItem> newSelect = new ArrayList<SelectItem>();

		for (SelectItem selectItem : query.getSelect()) {
			SelectItem latestSI = new SelectItem(selectItem);
			if (selectItem.getExpression() != null) {
				latestSI.setExpression(makeLatestExpSelConst(selectItem.getExpression(), queryProps, aliases, mapping));
			}
			newSelect.add(latestSI);
		}
		return newSelect;
	}

	/**
	 * Creates a new TableReference list for the LATEST equivalent of the given query.
	 * 
	 * @param query
	 *            SQL HISTORY query.
	 * @param aliases
	 *            Set of alias names.
	 * @return A new TableReference list for the LATEST equivalent of the given query.
	 * @throws DatabaseException
	 */
	private static List<TableReference> mapFrom(SelectStatement query, QueryProperties queryProps, final Set<String> aliases, Map<String, String> mapping) {
		List<TableReference> newFrom = new ArrayList<TableReference>();
		for (TableReference reference : query.getFrom()) {
			TableNameAndAlias aliasName = reference.getTable();
			String fqTableName = aliasName.getVdbTableName();
			String sqlTableName = mapping.get(fqTableName);
			TableNameAndAlias latestAN = new TableNameAndAlias(sqlTableName);

			if (aliasName.getAlias() != null) {
				aliases.add(aliasName.getAlias());
				latestAN.setAlias(aliasName.getAlias());
			}
			newFrom.add(new TableReference(latestAN));
		}
		return newFrom;
	}

	/**
	 * Creates a new select statement from the given query with all table names mapped using <code>mapping</code>.
	 * 
	 * @param query
	 *            SQL SELECT to convert.
	 * @param mapping
	 *            A table name mapping.
	 * @return A copy of the query with table names mapped.
	 * @throws DatabaseException
	 */
	private static SelectStatement mapSelectTables(SelectStatement query, QueryProperties queryProps, Map<String, String> mapping) {
		SelectStatement result = new SelectStatement();

		Set<String> aliases = new HashSet<String>();

		List<TableReference> newFrom = mapFrom(query, queryProps, aliases, mapping);
		result.addFrom(newFrom);

		List<SelectItem> newSelect = makeLatestSelect(query, queryProps, aliases, mapping);
		result.addSelect(newSelect);

		GroupByHaving newGroupBy = makeLatestGroupBy(query, queryProps, aliases, mapping);
		result.addGroupBy(newGroupBy);

		List<OrderBy> newOrderBy = makeLatestOrderBy(query, queryProps, aliases, mapping);
		result.addOrderBy(newOrderBy);

		if (query.getWhere() != null) {
			result.addWhere(makeLatestExpSelConst(query.getWhere(), queryProps, aliases, mapping));
		}

		return result;
	}

	/** Reference to access underlying database. */
	private TupleStoreDatabase m_databaseInstance;

	/** Details of this tuple store. */
	private TupleStoreDetails m_details;

	/** When this is exceeded an error is thrown */
	private long m_maxHistoryTuples;

	/** Mapping from vdbTableName to VdbTable */
	private Map<String, VdbTable> m_vdbTables;

	private StreamingSender m_streamingSender;

	/**
	 * Creates a new TupleStore.
	 * 
	 * @param databaseInstance
	 *            Reference to underlying database.
	 * @param tupleStores
	 *            Reference to list of tuple stores.
	 * @param details
	 *            Details of this tuple store.
	 * @param sender
	 */
	public TupleStore(TupleStoreDatabase databaseInstance, TupleStoreDetails details, long maxHistoryTuples, StreamingSender streamingSender) {
		m_databaseInstance = databaseInstance;
		m_details = details;
		m_maxHistoryTuples = maxHistoryTuples;
		m_vdbTables = new HashMap<String, VdbTable>();
		m_streamingSender = streamingSender;
		if (LOG.isInfoEnabled()) {
			LOG.info("TupleStore created: " + m_details);
		}
	}

	/**
	 * Deletes Tuples by HRP & last read Tuple ID for the given table. The value returned is the number of tuples
	 * deleted.
	 */
	public int cleanUpHRP(String vdbTableName) throws RGMAPermanentException {
		VdbTable vdbTable = getVdbTable(vdbTableName);
		int tupleUID = -1;
		String physicalTableName = vdbTable.m_historyTableName;
		synchronized (vdbTable.m_consumerTUIDs) {
			if (!m_details.isTemporary()) {
				m_databaseInstance.storeConsumerTUIDs(physicalTableName, vdbTable.m_consumerTUIDs);
			}
			for (int consumerTUID : vdbTable.m_consumerTUIDs.values()) {
				if (tupleUID == -1 || consumerTUID < tupleUID) {
					tupleUID = consumerTUID;
				}
			}
		}
		int nDel;
		if (tupleUID == 0) {
			/** Consumer known which has not yet received any tuples */
			nDel = 0;
		} else {
			nDel = m_databaseInstance.deleteByHRP(physicalTableName, vdbTable.m_hrpSecs, tupleUID);
		}
		int currentNooftuples = m_databaseInstance.count(physicalTableName);
		synchronized (vdbTable) {
			vdbTable.m_historyCount = currentNooftuples;
		}
		return nDel;
	}

	public void close() throws RGMAPermanentException {
		boolean permanent = !m_details.isTemporary();
		if (permanent) {
			cleanUpTables();
		}
		List<String> physicalTableNames = new ArrayList<String>();
		for (VdbTable vdbTable : m_vdbTables.values()) {
			physicalTableNames.add(vdbTable.m_historyTableName);
			if (vdbTable.m_latestTableName != null) {
				physicalTableNames.add(vdbTable.m_latestTableName);
			}
		}
		m_databaseInstance.closeTupleStore(physicalTableNames, permanent);
	}

	/**
	 * Creates a table in this tuple store with the specified format and history retention period.
	 */
	public void createTable(String vdbName, CreateTableStatement createTableStmt, int hrpSecs, List<String> authorizationList) throws RGMAPermanentException,
			RGMAPermanentException, RGMAPermanentException {
		String vdbTableName = (vdbName + "." + createTableStmt.getTableName()).toUpperCase();
		synchronized (m_vdbTables) {
			if (m_vdbTables.containsKey(vdbTableName)) {
				throw new RGMAPermanentException("createTable already called for " + vdbName + " " + createTableStmt);
			}
		}

		/* Build a CreateIndexStatement */
		CreateIndexStatement cis = new CreateIndexStatement("RgmaPrimaryKey");
		List<String> indexColumnNames = new ArrayList<String>();
		for (ColumnDefinition def : createTableStmt.getColumns()) {
			if (def.isPrimaryKey()) {
				indexColumnNames.add(def.getName());
			}
		}
		cis.setColumnNames(indexColumnNames);

		VdbTable vdbTable = new VdbTable();
		String histContTableName = m_databaseInstance.createTable(m_details.getOwnerDN(), m_details.getLogicalName(), vdbTableName, "H", createTableStmt);
		vdbTable.m_historyTableName = histContTableName;
		cis.setTableName(histContTableName);
		m_databaseInstance.createIndex(cis);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Mapped HISTORY table \"" + vdbTableName + "\" to \"" + histContTableName + "\".");
		}
		String latestTableName = "";
		if (m_details.supportsLatest()) {
			latestTableName = m_databaseInstance.createTable(m_details.getOwnerDN(), m_details.getLogicalName(), vdbTableName, "L", createTableStmt);
			vdbTable.m_latestTableName = latestTableName;
			cis.setTableName(latestTableName);
			m_databaseInstance.createIndex(cis);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Mapped LATEST table \"" + vdbTableName + "\" to \"" + latestTableName + "\".");
			}
		}
		vdbTable.m_consumerTUIDs = m_databaseInstance.getConsumerTUIDs(histContTableName);
		vdbTable.m_authz = authorizationList;
		vdbTable.m_TUID = m_databaseInstance.getMaxTUID(histContTableName);
		vdbTable.m_hrpSecs = hrpSecs;
		vdbTable.m_columns = createTableStmt.getColumns();
		synchronized (m_vdbTables) {
			m_vdbTables.put(vdbTableName, vdbTable);
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Table created in [" + m_details + "] " + createTableStmt);
		}
	}

	public long getHistoryCount(String vdbTableName) throws RGMAPermanentException {
		VdbTable vdbTable = getVdbTable(vdbTableName);
		synchronized (vdbTable) {
			return vdbTable.m_historyCount;
		}
	}

	/**
	 * Inserts a tuple into this tuple store. At this stage the insert statements have already been checked against the
	 * table schema.
	 * 
	 * @param tuples
	 *            List of insert statements (NB: for performance reasons, the InsertStatement objects may be changed by
	 *            this method).
	 * @param lrpSec
	 *            Latest retention period (in seconds).
	 */
	public void insert(UserContext context, final InsertStatement insert) throws RGMAPermanentException, BufferFullException {

		TableName ctn = insert.getTableName();
		String vdbTableName = ctn.getVdbTableName();
		VdbTable vdbTable = getVdbTable(vdbTableName);

		synchronized (vdbTable) {
			if (m_details.isMemory() && vdbTable.m_historyCount >= m_maxHistoryTuples) {
				throw new BufferFullException("Buffer is full. Please try again after a suitable delay.");
			}
		}

		if (m_details.supportsLatest()) {
			synchronized (this) {
				insert.setTable(new TableName(vdbTable.m_latestTableName));
				UpdateStatement update = createLatestUpdate(vdbTable.m_columns, insert);
				if (m_databaseInstance.update(update) == 0) {
					/*
					 * This can be zero for two reasons: - no tuple with the same primary key exists. - a tuple exists
					 * with the same private key but a newer timestamp, so no update was made. So must find out which.
					 */
					SelectStatement check = new SelectStatement();
					List<TableReference> from = new ArrayList<TableReference>(1);
					from.add(new TableReference(insert.getTableName()));
					check.addFrom(from);
					List<SelectItem> sis = new ArrayList<SelectItem>(1);
					sis.add(new SelectItem("*"));
					check.addSelect(sis);
					check.addWhere(((Expression) update.getWhere()).getOperand(0));
					TupleSetWithLastTUID rs = m_databaseInstance.select(check);
					TupleSet ts = rs.getTupleSet();
					if (ts.getData().size() == 0) {
						m_databaseInstance.insert(insert);
					}
				}
			}
		}

		/* Add extra columns to the history table */
		List<String> names = insert.getColumnNames();
		names.add(ReservedColumns.RGMA_INSERT_TIME_COLUMN_NAME);
		names.add(ReservedColumns.RGMA_TUID_COLUMN_NAME);
		String dateString = new Timestamp(System.currentTimeMillis()).toString();
		long uniqueID;
		synchronized (vdbTable) {
			uniqueID = ++vdbTable.m_TUID;
		}
		List<Constant> values = insert.getColumnValues();
		values.add(new Constant("'" + dateString + "'", Constant.Type.UNKNOWN));
		values.add(new Constant(uniqueID + "", Constant.Type.NUMBER));

		insert.setTable(new TableName(vdbTable.m_historyTableName));
		m_databaseInstance.insert(insert);
		synchronized (vdbTable) {
			vdbTable.m_historyCount++;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Inserted tuple into [" + m_details + "]");
		}
		m_streamingSender.dataAddedToTupleStore(this);
	}

	/**
	 * Opens a cursor on this tuple store for the specified query.
	 */
	public TupleCursor openCursor(SelectStatement query, QueryProperties queryProps, long startTimeMS, UserSystemContext context, ResourceEndpoint consumer)
			throws QueryTypeNotSupportedException, RGMAPermanentException, NumericException {

		TupleCursor result;
		Expression authPredicate;
		try {
			if (queryProps.isLatest()) {
				if (!m_details.supportsLatest()) {
					throw new QueryTypeNotSupportedException(queryProps);
				}
			}
			List<String> queryAuthorizationRules = new ArrayList<String>();
			List<String> queryAuthorizationTables = new ArrayList<String>();
			Map<String, String> historyTableNameMappings = new HashMap<String, String>();
			Map<String, String> latestTableNameMappings = new HashMap<String, String>();
			String vdbTableName = null;
			VdbTable vdbTable = null;

			for (TableReference reference : query.getFrom()) {
				vdbTableName = reference.getTable().getVdbTableName();
				vdbTable = getVdbTable(vdbTableName);
				List<String> authz = vdbTable.m_authz;
				if (authz == null) {
					throw new RGMAPermanentException("Consumer is accessing tuple store for which no producer currently exists.");
				}
				queryAuthorizationRules.addAll(authz);
				for (int i = 0; i < authz.size(); i++) {
					queryAuthorizationTables.add(vdbTableName);
				}
				historyTableNameMappings.put(vdbTableName, vdbTable.m_historyTableName);
				latestTableNameMappings.put(vdbTableName, vdbTable.m_latestTableName);
			}

			/* Get authorization predicate */
			authPredicate = Authz.constructAuthPredicate(queryAuthorizationTables, context.getDN(), context.getFQANs(), queryAuthorizationRules,
					Authz.RuleType.DATA, 'R');
			if (queryProps.isContinuous()) {
				query = addAuthPredicate(query, authPredicate);
				SelectStatement contQuery = mapSelectTables(query, queryProps, historyTableNameMappings);
				/*
				 * for the continuous query the vdbTableName and vdbTable will be left set correctly from the block
				 * above
				 */
				int lastTUID = 0;
				synchronized (vdbTable.m_consumerTUIDs) {
					Integer lt = vdbTable.m_consumerTUIDs.get(consumer);
					if (lt != null) {
						lastTUID = lt.intValue();
					}
				}
				result = new ContinuousTupleCursor(contQuery, startTimeMS, m_databaseInstance, vdbTableName, lastTUID);
			} else if (queryProps.isLatest()) {
				if (!m_details.supportsLatest()) {
					throw new QueryTypeNotSupportedException(queryProps);
				}
				query = addAuthPredicate(query, authPredicate);
				query = getLatestSelectStatement(query);
				SelectStatement latestQuery = mapSelectTables(query, queryProps, latestTableNameMappings);
				result = new OneTimeTupleCursor(latestQuery, m_databaseInstance);
			} else if (queryProps.isHistory()) {
				query = addAuthPredicate(query, authPredicate);
				SelectStatement historyQuery = mapSelectTables(query, queryProps, historyTableNameMappings);
				result = new OneTimeTupleCursor(historyQuery, m_databaseInstance);
			} else { // STATIC
				query = addAuthPredicate(query, authPredicate);
				SelectStatement staticQuery = mapSelectTables(query, queryProps, historyTableNameMappings);

				result = new OneTimeTupleCursor(staticQuery, m_databaseInstance);
			}
		} catch (ParseException e) {
			throw new RGMAPermanentException(e.getMessage(), e);
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Open cursor for " + query + " in [" + m_details + "]");
		}
		return result;
	}

	/**
	 * Store ConsumerID,TupleID,Table Name in HashMap
	 * 
	 * @param tableName
	 * @param consumerEndpoint
	 * @param rgmaTUID
	 * @throws RGMAPermanentException
	 */
	public void setConsumerTUID(String vdbTableName, ResourceEndpoint consumerEndpoint, int rgmaTUID) throws RGMAPermanentException {
		VdbTable vdbTable = getVdbTable(vdbTableName);
		synchronized (vdbTable.m_consumerTUIDs) {
			vdbTable.m_consumerTUIDs.put(consumerEndpoint, rgmaTUID);
		}
	}

	@Override
	public String toString() {
		return m_details.toString();
	}

	/**
	 * Called when producer finds a new set of consumers
	 * 
	 * @param tableName
	 * @param consumerList
	 * @throws RGMAPermanentException
	 */
	public void updateConsumerList(String vdbTableName, Set<ResourceEndpoint> consumerSet) throws RGMAPermanentException {
		VdbTable vdbTable = getVdbTable(vdbTableName);
		synchronized (vdbTable.m_consumerTUIDs) {
			Set<ResourceEndpoint> oldConsumerSet = new HashSet<ResourceEndpoint>(vdbTable.m_consumerTUIDs.keySet());
			oldConsumerSet.removeAll(consumerSet);
			for (ResourceEndpoint consumer : oldConsumerSet) {
				vdbTable.m_consumerTUIDs.remove(consumer);
			}
		}
	}

	/**
	 * Use to cleanup all tables associated with this tuplestore
	 * 
	 * @throws RGMAPermanentException
	 */
	void cleanUpTables() throws RGMAPermanentException {
		Set<Entry<String, VdbTable>> vtes;
		synchronized (m_vdbTables) {
			vtes = m_vdbTables.entrySet();
		}
		for (Entry<String, VdbTable> vte : vtes) {
			String vdbTableName = vte.getKey();
			VdbTable vdbTable = vte.getValue();
			int countH = cleanUpHRP(vdbTableName);
			int countL = 0;
			if (vdbTable.m_latestTableName != null) {
				countL = m_databaseInstance.deleteByLRP(vdbTable.m_latestTableName);
			}
			if (LOG.isInfoEnabled()) {
				if (vdbTable.m_latestTableName == null) {
					LOG.info("Cleaned up table " + vdbTableName + " and removed " + countH + " history tuples");
				} else {
					LOG.info("Cleaned up table " + vdbTableName + " and removed " + countH + " history tuples and " + countL + " latest tuples.");
				}
			}
		}
	}

	/**
	 * Gets the details of this tuple store.
	 * 
	 * @return Details of this tuple store.
	 */
	TupleStoreDetails getDetails() {
		return m_details;
	}

	private SelectStatement addAuthPredicate(SelectStatement q, Expression authPredicate) {
		if (q.getWhere() != null) {
			Expression authWhere = new Expression("AND", q.getWhere(), authPredicate);
			q.addWhere(authWhere);
		} else {
			q.addWhere(authPredicate);
		}
		return q;
	}

	private VdbTable getVdbTable(String vdbTableName) throws RGMAPermanentException {
		synchronized (m_vdbTables) {
			VdbTable vdbTable = m_vdbTables.get(vdbTableName);
			if (vdbTable == null) {
				throw new RGMAPermanentException(vdbTableName + " is not known to this tuple store");
			}
			return vdbTable;
		}
	}

	private class VdbTable {
		/** Authz rules */
		List<String> m_authz;

		/** ColumnDefintions for the table */
		List<ColumnDefinition> m_columns;

		/** TUID last streamed to that consumer resource */
		Map<ResourceEndpoint, Integer> m_consumerTUIDs;

		/** Count of tuples in history store. */
		long m_historyCount;

		/** Physical history table name */
		String m_historyTableName;

		/** hrpSecs for the HISTORY table */
		int m_hrpSecs;

		/** Physical latest table name */
		String m_latestTableName;

		/** Next TUID to be assigned is one more than this */
		long m_TUID;
	}

}
