/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.registry;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.database.MySQLConnection;
import org.glite.rgma.server.services.schema.SchemaService;
import org.glite.rgma.server.services.sql.ColumnValue;
import org.glite.rgma.server.services.sql.Constant;
import org.glite.rgma.server.services.sql.DataType;
import org.glite.rgma.server.services.sql.ProducerPredicate;
import org.glite.rgma.server.services.sql.DataType.Type;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.ConsumerEntry;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.SchemaColumnDefinition;
import org.glite.rgma.server.system.SchemaTableDefinition;

/**
 * MySQL implementation of the RegistryDatabase
 */
class MySQLRegistryDatabase implements RegistryDatabase {

	static final String CONSUMERS = "consumers";

	static final String INCOMING_REPLICATION = "incomingReplication";

	static final String PRODUCERS = "producers";

	private static final Logger LOG = Logger.getLogger(RegistryConstants.REGISTRY_DATABASE_LOGGER);
	private static final Logger LOG_CLEANUP = Logger.getLogger(RegistryConstants.REGISTRY_CLEANUP_LOGGER);

	private String m_consumersTableName;

	private final Object m_dbLock = new Object();

	private String m_fixedIntTableName;

	private String m_fixedRealTableName;

	private String m_fixedStringTableName;

	private String m_incomingReplicationTableName;

	private String m_producersTableName;

	private int m_registry_replication_lag = 0;

	// private MySQLConnection m_connection;
	private SchemaService m_schema = null;

	/**
	 * The namespace of the vdb that this registry services
	 */
	private String m_vdbName = null;

	private String m_hostName;

	/**
	 * Creates a new MySQLRegistryDatabase object.
	 * 
	 * @param vdbName
	 *            the name of the virtual database
	 * @throws RGMAPermanentException
	 * @throws ConfigurationException
	 *             if the database configuration could not be found
	 * @throws RegistryException
	 *             if a connection the the Mysql instance could not be established
	 * @throws SQLException
	 */
	MySQLRegistryDatabase(String vdbName) throws RGMAPermanentException {
		MySQLConnection.init();
		m_vdbName = vdbName;
		String prefix = vdbName + "_Registry_";

		ServerConfig serverConfig = ServerConfig.getInstance();
		m_registry_replication_lag = serverConfig.getInt(ServerConstants.REGISTRY_REPLICATION_LAG_SECS);
		m_hostName = serverConfig.getString(ServerConstants.SERVER_HOSTNAME);
		/* construct the table names for databases access table name */
		m_fixedStringTableName = prefix + RegistryConstants.FIXED_STRING_COLUMNS;
		m_fixedIntTableName = prefix + RegistryConstants.FIXED_INT_COLUMNS;
		m_fixedRealTableName = prefix + RegistryConstants.FIXED_REAL_COLUMNS;
		m_incomingReplicationTableName = prefix + INCOMING_REPLICATION;
		m_consumersTableName = prefix + "consumers";
		m_producersTableName = prefix + "producers";

		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			java.sql.ResultSet rs = con.executeQuery("SHOW TABLES LIKE '" + prefix + "%'");
			int nExpected = 6;
			List<String> tableNames = new ArrayList<String>(nExpected);
			while (rs.next()) {
				tableNames.add(rs.getString(1));
			}
			int n = tableNames.size();
			if (n == 0) {
				createTables(con);
			} else if (n != nExpected) {
				LOG.error("There are only " + n + " of the expected " + nExpected + " tables in the registry. Will attempt to recreate them");
				for (String tableName : tableNames) {
					con.executeUpdate("DROP TABLE " + tableName);
				}
				createTables(con);
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public void addRegistration(RegistryConsumerResource resource, boolean isMaster) throws RGMAPermanentException {
		registerResource(resource, m_consumersTableName, isMaster, -1);
	}

	public void addRegistration(RegistryProducerResource resource, boolean isMaster) throws RGMAPermanentException {
		registerResource(resource, m_producersTableName, isMaster, resource.getHrpSecs());
		addFixedColumns(resource);
	}

	/**
	 * Flags a registration as deleted
	 * 
	 * @param entry
	 *            the entry representing the resource to be deleted
	 * @throws SQLException
	 * @throws RegistryException
	 *             if a problem is encountered updating the registration
	 */

	public void deleteRegistration(UnregisterConsumerResourceRequest resource) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			con.executeUpdate("DELETE FROM " + m_consumersTableName + " " + "where ID = '" + resource.getEndpoint().getResourceID() + "' " + "and URL ='"
					+ resource.getEndpoint().getURL() + "' ");
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}

	}

	public void deleteRegistration(UnregisterProducerTableRequest resource) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			String[] tables = { m_fixedIntTableName, m_fixedRealTableName, m_fixedStringTableName };
			for (String table : tables) {
				con.executeUpdate("DELETE FROM " + table + " where URL = '" + resource.getEndpoint().getURL() + "' and ID = "
						+ resource.getEndpoint().getResourceID());
			}
			con.executeUpdate("DELETE FROM " + m_producersTableName + " " + "where ID = '" + resource.getEndpoint().getResourceID() + "' " + "and URL ='"
					+ resource.getEndpoint().getURL() + "' " + "and tableName = '" + resource.getTableName() + "'");
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public List<ProducerTableEntry> getAllProducersForTable(String tableName) throws RGMAPermanentException {
		StringBuilder query = new StringBuilder();
		query.append("SELECT p.URL, p.ID,p.isContinuous, p.isStatic, p.isHistory, p.isLatest, ");
		query.append("p.isSecondary, p.tableName, p.HRPSecs, p.predicate FROM ");
		query.append(m_producersTableName);
		query.append(" p WHERE p.tableName = '");
		query.append(tableName);
		query.append("' and (p.lastContactTimeSec + p.TISecs) > ");
		query.append(System.currentTimeMillis() / 1000);

		try {
			return getProducerTableEntries(query.toString());
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		}
	}

	public List<ConsumerEntry> getConsumersMatchingPredicate(RegistryProducerResource producer) throws RGMAPermanentException {
		List<ConsumerEntry> consumerEntries = new LinkedList<ConsumerEntry>();
		StringBuffer query = new StringBuffer();

		query.append("Select * from ");
		query.append(m_consumersTableName);
		query.append(" where tableName = '");
		query.append(producer.getTableName());
		if (producer.isSecondary()) {
			query.append("' AND isSecondary = 'N");
		}
		query.append("' AND (lastContactTimeSec + TISecs) > ");
		query.append(System.currentTimeMillis() / 1000);
		ProducerPredicate producerPredicate = null;
		try {
			producerPredicate = ProducerPredicate.parse(producer.getPredicate());
		} catch (ParseException e) {
			throw new RGMAPermanentException("Producer predicate", e);
		}
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			ResultSet rs = con.executeQuery(query.toString());
			while (rs.next()) {
				String predicate = rs.getString("predicate");
				ResourceEndpoint endpoint = new ResourceEndpoint(new URL(rs.getString("URL")), rs.getInt("ID"));
				if (!predicate.equals("")) {
					ConsumerPredicate consumerPredicate = new ConsumerPredicate(predicate);
					if (checkPredicatesMatch(consumerPredicate, producerPredicate)) {
						// create a ConsumerEntry
						ConsumerEntry consumerEntry = new ConsumerEntry(endpoint);
						consumerEntries.add(consumerEntry);
					} else {
						if (LOG.isDebugEnabled()) {
							StringBuffer msg = new StringBuffer();
							msg.append("predicates don't match ");
							msg.append(consumerPredicate.toString());
							msg.append(" : ");
							msg.append(producerPredicate.toString());

							LOG.debug(msg.toString());
						}
					}
				} else {// predicate must match as the consumer predicate is ""
					ConsumerEntry consumerEntry = new ConsumerEntry(endpoint);
					consumerEntries.add(consumerEntry);
				}
			}
			con.close();
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}

		return consumerEntries;
	}

	public String getConsumerTableEntriesForHostName(String hostName) throws RGMAPermanentException {
		return getEntries("SELECT * FROM " + m_consumersTableName + " WHERE URL LIKE '%//" + hostName + ":%'");
	}

	public String getConsumerTableEntriesForTableName(String tableName) throws RGMAPermanentException {
		return getEntries("SELECT * FROM " + m_consumersTableName + " WHERE tableName='" + tableName + "'");
	}

	public ReplicationMessage getFullUpdateRecords(int now) throws RGMAPermanentException {
		return getFullReplica(now);
	}

	public int getLastReplicationTimeSecs(String hostname) throws RGMAPermanentException {
		MySQLConnection con = null;
		int lastReplicationTime = 0;
		con = new MySQLConnection();
		try {
			ResultSet res = con.executeQuery("SELECT lastReplicationTimeSec FROM " + m_incomingReplicationTableName + " WHERE hostname = '" + hostname + "'");
			if (res.first()) {
				lastReplicationTime = res.getInt("lastReplicationTimeSec");
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return lastReplicationTime;
	}

	/**
	 * Gets all producers (of the required table(s)/type(s)) that don't contradict the predicate and registers
	 * continuous consumers. for now this will only handle simple queries. If there is an OR, LIKE IN etc. all producers
	 * for the table will be returned.
	 * 
	 * @param resource
	 *            the consumer to be registered
	 * @return ProducerTableEntryList containing all producers (of the required table/type) that don't contradict the
	 *         predicate
	 * @throws RegistryException
	 *             if a problem is encountered
	 */
	public List<ProducerTableEntry> getProducersMatchingPredicate(List<String> tableNames, String predicate, QueryProperties queryProperties,
			boolean isSecondary) throws RGMAPermanentException {
		try {
			ConsumerPredicate p = new ConsumerPredicate(predicate);
			List<ColumnOperatorValue> columns = p.getPredicateColumns();

			List<StringBuilder> sqlQueries = new LinkedList<StringBuilder>();

			// there may be more than one table if the consumer consumes from a
			// secondary producer
			for (int index = 0; index < tableNames.size(); index++) {
				StringBuilder sql = new StringBuilder();
				sql.append("SELECT r.URL, r.ID, r.isContinuous, r.isStatic, r.isHistory, r.isLatest, ");
				sql.append("r.isSecondary, r.tableName, r.HRPSecs, r.predicate FROM ");
				sql.append(m_producersTableName);
				sql.append(" r ");
				sql.append("WHERE r.tableName = '");
				sql.append(tableNames.get(index));
				if (queryProperties.isContinuous()) {
					sql.append("' AND r.isContinuous ='Y");
				}
				if (queryProperties.isStatic()) {
					sql.append("' AND r.isStatic ='Y");
				}
				if (queryProperties.isHistory()) {
					sql.append("' AND r.isHistory ='Y");
				}
				if (queryProperties.isLatest()) {
					sql.append("' AND r.isLatest ='Y");
				}
				if (isSecondary) {
					/* Secondary consumers must not get secondary producers */
					sql.append("' AND r.isSecondary = 'N");
				}
				// only want producers
				sql.append("' AND (r.lastContactTimeSec + r.TISecs) > ");
				sql.append(System.currentTimeMillis() / 1000);
				sql.append(" ");
				StringBuilder sqlnopredicate = new StringBuilder(sql);

				sqlQueries.add(sqlnopredicate);

				HashMap<String, SchemaColumnDefinition> colDefList = null;

				p.checkQueryIsSimple();
				// if there is a predicate create the exists clauses
				if (!columns.isEmpty()) {
					// query will match any without a predicate as well as those
					// with a
					// predicate
					sql.append("AND r.predicate = ''");
					try {
						colDefList = getColumnTypesFromSchema(tableNames.get(index), columns);
					} catch (RGMAPermanentException e) {
						throw new RGMAPermanentException(e);
					}

					// an array of column values used to create the exists
					// clause
					// they will all have the same table name
					List<ColumnOperatorValue> columnsToQuery = new LinkedList<ColumnOperatorValue>();

					List<String> columnNames = new LinkedList<String>();

					// get a list of names for the columns this ensures there is
					// only one entry
					// for
					// each column name found
					for (int i = 0; i < columns.size(); i++) {
						if (!columnNames.contains(columns.get(i).getName())) {
							columnNames.add(columns.get(i).getName());
						}
					}

					// create a list of operators and values for the column name
					for (int i = 0; i < columnNames.size(); i++) {
						// get all of the columns in the predicate with the same
						// name
						columnsToQuery.clear();

						// add all of the column-operator-values for the column
						// name
						for (int r = 0; r < columns.size(); r++) {
							if (columns.get(r).getName().equals(columnNames.get(i))) {
								columnsToQuery.add(columns.get(r));
							}
						}

						for (int z = 0; z < columnsToQuery.size(); z++) {
							StringBuilder sqlclone = new StringBuilder(sql);
							sqlclone.append(" AND ");
							DataType type = null;
							type = getColumnType(columnNames.get(i), colDefList);
							List<ColumnOperatorValue> l = new LinkedList<ColumnOperatorValue>();
							l.add(columnsToQuery.get(z));
							sqlclone.append(createExistsClause(type, l));
						}
					}
					sqlQueries.add(sql);
				}
			}

			StringBuffer query = new StringBuffer();

			// if only one query has been generated get that otherwise union the
			// queries together
			if (sqlQueries.size() == 1) {
				query.append(sqlQueries.get(0));
			} else {
				for (int i = 0; i < sqlQueries.size(); i++) {
					query.append("(");
					query.append(sqlQueries.get(i));
					query.append(")");

					if (i + 1 < sqlQueries.size()) {
						query.append("UNION");
					}
				}
			}

			return getProducerTableEntries(query.toString());
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		}
	}

	public String getProducerTableEntriesForHostName(String hostName) throws RGMAPermanentException {
		return getEntries("SELECT * FROM " + m_producersTableName + " WHERE URL LIKE '%//" + hostName + ":%'");
	}

	public String getProducerTableEntriesForTableName(String tableName) throws RGMAPermanentException {
		return getEntries("SELECT * FROM " + m_producersTableName + " WHERE tableName='" + tableName + "'");
	}

	public int getRegisteredConsumerCount(boolean masterOnly) throws RGMAPermanentException {
		int producers = 0;
		StringBuilder query = new StringBuilder();
		query.append("select count(*) from ");
		query.append(m_consumersTableName);
		if (masterOnly) {
			query.append(" WHERE isMaster = 'Y'");
		}
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			con = new MySQLConnection();
			ResultSet rs = con.executeQuery(query.toString());
			rs.first();
			producers = rs.getInt("count(*)");
		} catch (SQLException e) {
			throw new RGMAPermanentException("Exception caught when getting RegisteredConsumerCount", e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return producers;
	}

	public int getRegisteredProducerCount(boolean masterOnly) throws RGMAPermanentException {
		int producers = 0;
		StringBuilder query = new StringBuilder();
		query.append("select count(*) from ");
		query.append(m_producersTableName);
		if (masterOnly) {
			query.append(" WHERE isMaster = 'Y'");
		}
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			con = new MySQLConnection();
			ResultSet rs = con.executeQuery(query.toString());
			rs.first();
			producers = rs.getInt("count(*)");
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return producers;
	}

	public List<String> getUniqueHostNames() throws RGMAPermanentException {
		String query = "(SELECT DISTINCT URL from " + m_producersTableName + ") UNION ( SELECT DISTINCT URL from " + m_consumersTableName + ")";
		MySQLConnection con = null;
		Set<String> names = new HashSet<String>();
		try {
			con = new MySQLConnection();
			con = new MySQLConnection();
			ResultSet rs = con.executeQuery(query);
			while (rs.next()) {
				URL url = new URL(rs.getString(1));
				names.add(url.getHost());
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return new ArrayList<String>(names);
	}

	public List<String> getUniqueTableNames() throws RGMAPermanentException {
		String query = "(SELECT DISTINCT tableName from " + m_producersTableName + ") UNION ( SELECT DISTINCT tableName from " + m_consumersTableName + ")";
		MySQLConnection con = null;
		List<String> names = new ArrayList<String>();
		try {
			con = new MySQLConnection();
			con = new MySQLConnection();
			ResultSet rs = con.executeQuery(query);
			while (rs.next()) {
				names.add(rs.getString(1));
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return names;
	}

	/**
	 * Deletes expired registrations from the database
	 * 
	 * @throws RegistryException
	 *             if a problem is encountered
	 */
	public void purgeExpiredRegistrations() throws RGMAPermanentException {
		// get all expired producers to that the associated fixed
		// columns(predicate) can be removed
		StringBuilder query = new StringBuilder("SELECT * FROM ");
		query.append(m_producersTableName);
		query.append(" WHERE (lastContactTimeSec + TISecs) < ");
		query.append(System.currentTimeMillis() / 1000);
		MySQLConnection con = null;
		ResultSet rs = null;

		try {
			con = new MySQLConnection();
			rs = con.executeQuery(query.toString());

			// remove all of the fixed string columns
			while (rs.next()) {
				String url = rs.getString("URL");
				int conId = rs.getInt("ID");
				String tableName = rs.getString("tableName");

				List<String> tables = new LinkedList<String>();
				tables.add(m_fixedIntTableName.toString());
				tables.add(m_fixedStringTableName.toString());
				tables.add(m_fixedRealTableName.toString());

				/*
				 * this operation needs to be locked so that any addRegistration calls are guaranteed to be atomic.
				 * addRegistration will do an update if the insert fails. If the record is deleted in between an 'add'
				 * and an 'update' the update will fail so they need to be atomic.
				 */
				synchronized (m_dbLock) {
					for (int i = 0; i < tables.size(); i++) {
						StringBuffer sqlquery = new StringBuffer();
						sqlquery.append("delete from ");
						sqlquery.append(tables.get(i));
						sqlquery.append(" where URL = '");
						sqlquery.append(url);
						sqlquery.append("' and ID = ");
						sqlquery.append(conId);
						sqlquery.append(" and tableName = '");
						sqlquery.append(tableName);
						sqlquery.append("'");
						con.executeUpdate(sqlquery.toString());

					}
				}
			}

			/* TODO this would appear to clean up more than it should - should it not act table by table or ... */
			int ops = con.executeUpdate("DELETE FROM " + m_producersTableName + " WHERE (lastContactTimeSec + TISecs) < " + System.currentTimeMillis() / 1000);
			if (LOG_CLEANUP.isDebugEnabled()) {
				LOG_CLEANUP.debug("Deleted " + ops + " rows cleaning up producers database for vdb " + m_vdbName);
			}

			ops = con.executeUpdate("DELETE FROM " + m_consumersTableName + " WHERE (lastContactTimeSec + TISecs) < " + System.currentTimeMillis() / 1000);
			if (LOG_CLEANUP.isDebugEnabled()) {
				LOG_CLEANUP.debug("Deleted " + ops + " rows cleaning up consumers database for vdb " + m_vdbName);
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException("Problem encountered extracting from result set " + e.getMessage(), e);
		} finally {
			if (con != null) {
				con.close();
			}
		}

	}

	public void setLastReplicationTime(String hostname, int time) throws RGMAPermanentException {
		MySQLConnection con = null;
		con = new MySQLConnection();
		LOG.debug("Setting LastReplicationTime to " + time + " for vdbName " + m_vdbName + " and host " + hostname);
		try {
			con.executeUpdate("REPLACE INTO " + m_incomingReplicationTableName + " values('" + hostname + "', " + time + ")");
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public void shutdown() {
		LOG.info("MySQLDatabase for VDB " + m_vdbName + " shutdown called");
	}

	/**
	 * add the fixed columns to the database representing the predicate of the producer
	 * 
	 * @param producer
	 *            the producer resource being added to the registry
	 * @throws RegistryException
	 *             wrapping database exception if problems occur
	 */
	private void addFixedColumns(RegistryProducerResource producer) throws RGMAPermanentException {
		// register the fixed columns if there are any
		if (producer.getPredicate() != null && !producer.getPredicate().equals("")) {
			MySQLConnection con = null;
			try {
				con = new MySQLConnection();
				ProducerPredicate producerPredicate = null;
				try {
					producerPredicate = ProducerPredicate.parse(producer.getPredicate());
				} catch (ParseException e) {
					throw new RGMAPermanentException("Predicate Parsing problem encountered", e);
				}

				// get the column values - the operator for producer predicates
				// can only be =
				List<ColumnValue> columnValues = producerPredicate.getColumnValues();

				HashMap<String, SchemaColumnDefinition> columnDefinitions;
				try {
					columnDefinitions = getColumnTypesFromSchema(producer.getTableName(), producerPredicate.getColumnValues());
				} catch (RGMAPermanentException e) {
					throw new RGMAPermanentException(e);
				}
				String table = null;

				// iterate through the colums and determine the type
				for (int i = 0; i < columnValues.size(); i++) {

					SchemaColumnDefinition def = columnDefinitions.get(columnValues.get(i).getName());

					if (def != null) {
						if (def.getType().getType().equals(Type.INTEGER)) {
							table = m_fixedIntTableName;
						} else if (def.getType().getType().equals(Type.REAL)) {
							table = m_fixedRealTableName;
						} else if (def.getType().getType().equals(Type.VARCHAR)) {
							table = m_fixedStringTableName;
						} else {
							throw new RGMAPermanentException("Incompatible Column Type " + def.getType().toString());
						}
					} else {
						throw new RGMAPermanentException("No valid column definitions returned from Schema ");
					}

					StringBuffer query = new StringBuffer();
					query.append("replace into ");
					query.append(table);
					query.append(" values ('");
					query.append(producer.getTableName());
					query.append("', '");
					query.append(producer.getEndpoint().getURL());
					query.append("', ");
					query.append(producer.getEndpoint().getResourceID());
					query.append(", '");
					query.append(columnValues.get(i).getName());
					query.append("', ");

					if (def.getType().getType().equals(Type.VARCHAR)) {
						// query.append("'");
						query.append(columnValues.get(i).getValue());
						// query.append("'");
					} else {
						query.append(columnValues.get(i).getValue());
					}

					query.append(" )");

					con.executeUpdate(query.toString());

				}
				con.close();
			} catch (SQLException e) {
				throw new RGMAPermanentException(e);
			} finally {
				if (con != null) {
					con.close();
				}
			}
		} else {
			// do nothing as there is no predicate
		}
	}

	/**
	 * check if the producer predicate contracicts the consumerPredicate
	 * 
	 * @param consumerPredicate
	 * @param producerPredicate
	 * @return true if predicates match( don't contradict)
	 * @throws RegistryException
	 *             if a problem is encountered
	 */
	private boolean checkPredicatesMatch(ConsumerPredicate consumerPredicate, ProducerPredicate producerPredicate) throws RGMAPermanentException {
		boolean result = false;

		// check the 2 predicates to see that they do not contradict.
		List<ColumnOperatorValue> consumerColumns = consumerPredicate.getPredicateColumns();
		List<ColumnValue> producerColumns = producerPredicate.getColumnValues();

		if (consumerColumns.isEmpty()) {
			result = true;
		} else {
			for (int i = 0; i < consumerColumns.size(); i++) {
				for (int index = 0; index < producerColumns.size(); index++) {
					// if the column names are the same we need to see if the
					// values match
					if (consumerColumns.get(i).getName().equals(producerColumns.get(index).getName())) {
						if (doColumnValuesContradict(consumerColumns.get(i), producerColumns.get(index))) {
							return false;
						}
					}
				}
			}

			result = true;
		}

		return result;
	}

	/**
	 * create an exists sub query
	 * 
	 * @param type
	 *            the type of the column
	 * @param columns
	 *            an array of the column operator values for a column
	 * @param isSimple
	 * @return an exists sub query
	 */
	private StringBuilder createExistsClause(DataType type, List<ColumnOperatorValue> columns) {
		StringBuilder sql = new StringBuilder();

		sql.append("EXISTS(SELECT * FROM ");

		String tblName = null;
		String alias = null;

		if (type.getType() == Type.VARCHAR) {
			tblName = m_fixedStringTableName;
			alias = " s";
		} else if (type.getType() == Type.INTEGER) {
			tblName = m_fixedIntTableName;
			alias = " i";
		} else if (type.getType() == Type.REAL) {
			tblName = m_fixedRealTableName;
			alias = " re";
		} else {
			LOG.equals("UNKNOWN type: " + type.getType());
			// error condition unknown type
		}

		sql.append(tblName);
		sql.append(alias);
		sql.append(" ");
		sql.append("WHERE r.tableName = ");
		sql.append(alias);
		sql.append(".tableName ");

		if (columns != null && columns.size() > 0) {
			sql.append("AND r.ID = ");
			sql.append(alias);
			sql.append(".ID ");
			sql.append("AND r.URL = ");
			sql.append(alias);
			sql.append(".URL ");
			sql.append(" AND ");
			sql.append(alias);
			sql.append(".columnName = '");
			// same column name for everything
			sql.append(columns.get(0).getName());
			sql.append("' ");

			for (int i = 0; i < columns.size() - 1; i++) {
				sql.append("AND ");
				sql.append(alias);
				sql.append(".value ");
				sql.append(columns.get(i).getOperator());
				sql.append(" ");
				sql.append(columns.get(i).getValue());
				sql.append(" ");
			}

			sql.append(")");
		}
		return sql;
	}

	private void createTables(MySQLConnection con) throws SQLException, RGMAPermanentException {
		StringBuilder query;

		query = new StringBuilder("CREATE TABLE ");
		query.append(m_consumersTableName);
		query.append("(tableName VARCHAR(128) NOT NULL,");
		query.append("ID INT NOT NULL,");
		query.append("URL VARCHAR(255) NOT NULL,");
		query.append("predicate TEXT,");
		query.append("TISecs INT NOT NULL,");
		query.append("isContinuous CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isHistory CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isLatest CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isStatic CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isSecondary CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isMaster CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("lastContactTimeSec BIGINT NOT NULL,");
		query.append("PRIMARY KEY( tableName, ID, URL)");
		query.append(")");
		con.executeUpdate(query.toString());

		query = new StringBuilder("CREATE TABLE ");
		query.append(m_producersTableName);
		query.append("(tableName VARCHAR(128) NOT NULL,");
		query.append("ID INT NOT NULL,");
		query.append("URL VARCHAR(255) NOT NULL,");
		query.append("predicate TEXT,");
		query.append("TISecs INT NOT NULL,");
		query.append("isContinuous CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isHistory CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isLatest CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isStatic CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("isSecondary CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("HRPSecs INT DEFAULT 0,");
		query.append("isMaster CHAR(1) DEFAULT 'N' NOT NULL,");
		query.append("lastContactTimeSec BIGINT NOT NULL,");
		query.append("PRIMARY KEY( tableName, ID, URL)");
		query.append(")");
		con.executeUpdate(query.toString());

		query = new StringBuilder("CREATE TABLE ");
		query.append(m_fixedIntTableName);
		query.append("(");
		query.append("tableName varchar(200) not null,");
		query.append("URL varchar(100) not null,");
		query.append("ID int(10) not null,");
		query.append("columnName varchar(50) not null,");
		query.append("value int(11),");
		query.append("primary key( tableName, URL, ID, columnName )");
		query.append(")");
		con.executeUpdate(query.toString());

		query = new StringBuilder("CREATE TABLE ");
		query.append(m_fixedRealTableName);
		query.append("(");
		query.append("tableName varchar(200) not null,");
		query.append("URL varchar(100) not null,");
		query.append("ID int(10) not null,");
		query.append("columnName varchar(50) not null,");
		query.append("value double,");
		query.append("primary key( tableName, URL, ID, columnName )");
		query.append(")");
		con.executeUpdate(query.toString());

		query = new StringBuilder("CREATE TABLE ");
		query.append(m_fixedStringTableName);
		query.append("(");
		query.append("tableName varchar(200) not null,");
		query.append("URL varchar(100) not null,");
		query.append("ID int(10) not null,");
		query.append("columnName varchar(50) not null,");
		query.append("value varchar(255) ,");
		query.append("primary key( tableName, URL, ID, columnName )");
		query.append(")");
		con.executeUpdate(query.toString());

		query = new StringBuilder("CREATE TABLE ");
		query.append(m_incomingReplicationTableName);
		query.append("(");
		query.append("hostname varchar(255),");
		query.append("lastReplicationTimeSec int(10) not null,");
		query.append("primary key(hostname)");
		query.append(")");
		con.executeUpdate(query.toString());

	}

	/**
	 * does the column value contradict this Colum operator value The ColumnValue is a producer predicate( the operator
	 * is always =) This ColumnOperatorValue is the consumer predicate operators can be =,<>,>=,<=,<,>
	 * 
	 * @param columnValue
	 *            the ColumnValue to compare with this Columnoperator value
	 * @return true if the column value contradicts this column operator value false otherwise
	 * @throws RegistryException
	 *             if the Column names do not match or an unknown or not supported operator is encountered
	 */
	private boolean doColumnValuesContradict(ColumnOperatorValue columnOperatorValue, ColumnValue columnValue) throws RGMAPermanentException {
		boolean result = true;

		if (!columnValue.getName().equals(columnOperatorValue.getName())) {
			StringBuilder msg = new StringBuilder("ColumnNames do not match ");
			msg.append(columnValue.getName());
			msg.append(" expected ");
			msg.append(columnOperatorValue.getName());
			throw new RGMAPermanentException(msg.toString());
		}

		// if the column names match then the types must match
		// get the type from the columnValue
		// if its a string do a straight equals comparison
		if (columnValue.getValue().getType().compareTo(Constant.Type.STRING) == 0) {
			if (columnOperatorValue.getOperator().equals("=") && !columnValue.getValue().equals(columnOperatorValue.getValue())
					|| columnOperatorValue.getOperator().equals("<>") && columnValue.getValue().equals(columnOperatorValue.getValue())) {
				result = false;
			}
		} else // its a real or integer
		{
			// if it is parsed as a real(double) the comparison can be done even
			// if it is an integer
			double consumerValue = Double.parseDouble(columnOperatorValue.getValue());
			double producerValue = Double.parseDouble(columnValue.getValue().getValue());
			String operator = columnOperatorValue.getOperator();

			if (operator.equals("=")) {
				if (consumerValue == producerValue) {
					result = false;
				}
			} else if (operator.equals(">=")) {
				if (consumerValue >= producerValue) {
					result = false;
				}
			} else if (operator.equals("<=")) {
				if (consumerValue <= producerValue) {
					result = false;
				}
			} else if (operator.equals("<")) {
				if (consumerValue < producerValue) {
					result = false;
				}
			} else if (operator.equals(">")) {
				if (consumerValue > producerValue) {
					result = false;
				}
			} else if (operator.equals("<>")) {
				if (consumerValue != producerValue) {
					result = false;
				}
			} else {
				// unknown/not supported operator
				throw new RGMAPermanentException("Unknown or not supported operator: " + operator);
			}
		}

		return result;
	}

	/**
	 * find the column type
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param colDefinitions
	 *            a list of column definitions
	 * @return the type of the column
	 * @throws RegistryException
	 *             if a problem is encountered
	 */
	private DataType getColumnType(String columnName, HashMap<String, SchemaColumnDefinition> colDefinitions) throws RGMAPermanentException {
		// get the type of this column
		DataType type = null;

		SchemaColumnDefinition coldef = colDefinitions.get(columnName);
		if (coldef != null) {
			type = coldef.getType();
		} else {
			throw new RGMAPermanentException("Cannot find type for column: " + columnName);
		}

		return type;
	}

	/**
	 * get the column types from the schema
	 * 
	 * @param tableName
	 *            the table name to be lookup
	 * @param columnNames
	 *            the column names used in the predicate
	 * @return A list of all column types that map directly to columnNames
	 * @throws RGMAPermanentException
	 *             if a column name cannot be found in the table schema
	 * @throws RGMAPermanentException
	 *             if the schema cannot be contacted
	 */
	private HashMap<String, SchemaColumnDefinition> getColumnTypesFromSchema(String tableName, List<?> columns) throws RGMAPermanentException {
		if (!columns.isEmpty()) {
			HashMap<String, SchemaColumnDefinition> colDefList = new HashMap<String, SchemaColumnDefinition>();
			Object columnObject = columns.get(0);
			List<String> columnNames = new LinkedList<String>();

			if (columnObject instanceof ColumnOperatorValue) {
				for (int i = 0; i < columns.size(); i++) {
					columnNames.add(((ColumnOperatorValue) columns.get(i)).getName());
				}
			} else if (columnObject instanceof ColumnValue) {
				for (int i = 0; i < columns.size(); i++) {
					columnNames.add(((ColumnValue) columns.get(i)).getName());
				}
			} else {
				throw new RGMAPermanentException("Unknown Class Type " + columnObject.getClass().getName());
			}

			SchemaTableDefinition tableDef = null;

			LOG.debug("Retrieving table definition from the Schema");
			if (m_schema == null) {
				m_schema = SchemaService.getInstance();
			}
			String vdbNameToSend = m_vdbName;

			if (m_vdbName.equals("default")) {
				vdbNameToSend = "";
			}
			tableDef = m_schema.getTableDefinition(vdbNameToSend, tableName, null);

			List<SchemaColumnDefinition> schemaColumnList = tableDef.getColumns();

			// Create a hashmap of the column definitions to make searching of
			// the columns
			// by table name easier

			HashMap<String, SchemaColumnDefinition> schemaColumns = new HashMap<String, SchemaColumnDefinition>();

			for (int i = 0; i < schemaColumnList.size(); i++) {

				schemaColumns.put(schemaColumnList.get(i).getName().toUpperCase(), schemaColumnList.get(i));
			}

			for (int i = 0; i < columnNames.size(); i++) {
				SchemaColumnDefinition cd = schemaColumns.get(columnNames.get(i).toUpperCase());
				if (cd == null) {
					String errorMsg = "Column name not found in Schema: " + columnNames.get(i);

					throw new RGMAPermanentException(errorMsg);
				} else {
					colDefList.put(columnNames.get(i), cd);
				}
			}
			return colDefList;

		} else {
			throw new RGMAPermanentException("An empty list has been passed to getColumnTypesFromSchema() so can't get column types");
		}
	}

	private String getEntries(String query) throws RGMAPermanentException {
		StringBuilder xml = new StringBuilder();
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			con = new MySQLConnection();
			ResultSet rs = con.executeQuery(query);
			while (rs.next()) {
				xml.append("<Entry tableName=\"").append(rs.getString("tableName"));
				xml.append("\" ID=\"").append(rs.getString("ID"));
				xml.append("\" URL=\"").append(rs.getString("URL"));
				xml.append("\" predicate=\"").append(!rs.getString("predicate").equals(""));
				xml.append("\" TIMillis=\"").append(rs.getString("TISecs") + "000");
				xml.append("\" props=\"");
				if (rs.getString("isContinuous").equals("Y")) {
					xml.append('C');
				}
				if (rs.getString("isHistory").equals("Y")) {
					xml.append('H');
				}
				if (rs.getString("isLatest").equals("Y")) {
					xml.append('L');
				}
				if (rs.getString("isStatic").equals("Y")) {
					xml.append('S');
				}
				if (rs.getString("isSecondary").equals("Y")) {
					xml.append('2');
				}
				xml.append("\" isMaster=\"").append(rs.getString("isMaster").equals("Y"));
				xml.append("\" lastContactTimeMillis=\"").append(rs.getString("lastContactTimeSec") + "000");
				xml.append("\"/>");
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return xml.toString();
	}

	private ReplicationMessage getFullReplica(int now) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();

			ArrayList<RegistryProducerResource> ps = new ArrayList<RegistryProducerResource>();
			ResultSet rs = con.executeQuery("SELECT * FROM " + m_producersTableName + " WHERE isMaster = 'Y'");
			while (rs.next()) {
				ResourceEndpoint ep = new ResourceEndpoint(new URL(rs.getString(RegistryConstants.URL)), rs.getInt(RegistryConstants.ID));
				ProducerType type = new ProducerType(rs.getString(RegistryConstants.IS_HISTORY).equals("Y"), rs.getString(RegistryConstants.IS_LATEST).equals(
						"Y"), rs.getString(RegistryConstants.IS_CONTINUOUS).equals("Y"), rs.getString(RegistryConstants.IS_STATIC).equals("Y"), rs.getString(
						RegistryConstants.IS_SECONDARY).equals("Y"));
				RegistryProducerResource r = new RegistryProducerResource(rs.getString(RegistryConstants.TABLE_NAME), ep, rs
						.getString(RegistryConstants.PREDICATE), rs.getInt(RegistryConstants.TI_SECS), type, rs.getInt(RegistryConstants.HRP_SECS));
				ps.add(r);
			}

			ArrayList<RegistryConsumerResource> cs = new ArrayList<RegistryConsumerResource>();
			rs = con.executeQuery("SELECT * FROM " + m_consumersTableName + " WHERE isMaster = 'Y'");
			while (rs.next()) {
				ResourceEndpoint ep = new ResourceEndpoint(new URL(rs.getString(RegistryConstants.URL)), rs.getInt(RegistryConstants.ID));
				QueryProperties props = null;
				if (rs.getString(RegistryConstants.IS_HISTORY).equals("Y")) {
					props = QueryProperties.HISTORY;
				} else if (rs.getString(RegistryConstants.IS_LATEST).equals("Y")) {
					props = QueryProperties.LATEST;
				} else if (rs.getString(RegistryConstants.IS_CONTINUOUS).equals("Y")) {
					props = QueryProperties.CONTINUOUS;
				} else if (rs.getString(RegistryConstants.IS_STATIC).equals("Y")) {
					props = QueryProperties.STATIC;
				} else {
					throw new RGMAPermanentException("getFullReplica finds consumer entry without QueryProperties");
				}
				RegistryConsumerResource r = new RegistryConsumerResource(rs.getString(RegistryConstants.TABLE_NAME), ep, rs
						.getString(RegistryConstants.PREDICATE), rs.getInt(RegistryConstants.TI_SECS), props, rs.getString(RegistryConstants.IS_SECONDARY)
						.equals("Y"));
				cs.add(r);
			}

			ReplicationMessage replicationMessage = new ReplicationMessage(m_vdbName, m_hostName, now, 0, ps, cs,
					new ArrayList<UnregisterProducerTableRequest>(0), new ArrayList<UnregisterConsumerResourceRequest>(0));
			return replicationMessage;

		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	/**
	 * executes the query and returns a list of producer table entries that are registered in the registry
	 * 
	 * @param query
	 *            an sql query to be executed to retrieve the producer entries
	 * @return a list of producer table entries
	 * @throws RegistryException
	 *             ifConsumerEntryList a problem is encountered
	 */
	private List<ProducerTableEntry> getProducerTableEntries(String query) throws SQLException, RGMAPermanentException {

		List<ProducerTableEntry> producers = new LinkedList<ProducerTableEntry>();
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();

			ResourceEndpoint endpoint;
			ProducerType type;
			int retentionPeriod;
			String tableName;
			String predicate;

			java.sql.ResultSet rs = con.executeQuery(query);
			while (rs.next()) {
				endpoint = new ResourceEndpoint(rs.getURL("URL"), rs.getInt("ID"));

				boolean latest = false;
				boolean history = false;
				boolean continuous = false;
				boolean isStatic = false;
				boolean isSecondary = false;

				if (rs.getString("isHistory").equalsIgnoreCase("Y")) {
					history = true;
				}

				if (rs.getString("isContinuous").equalsIgnoreCase("Y")) {
					continuous = true;
				}

				if (rs.getString("isStatic").equalsIgnoreCase("Y")) {
					isStatic = true;
				}

				if (rs.getString("isSecondary").equalsIgnoreCase("Y")) {
					isSecondary = true;
				}

				if (rs.getString("isLatest").equalsIgnoreCase("Y")) {
					latest = true;
				}

				retentionPeriod = rs.getInt("HRPSecs");
				tableName = rs.getString("tableName");
				predicate = rs.getString("predicate");
				type = new ProducerType(history, latest, continuous, isStatic, isSecondary);

				ProducerTableEntry producer = new ProducerTableEntry(endpoint, m_vdbName, tableName, type, retentionPeriod, predicate);
				producers.add(producer);

			}
			con.close();
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}

		return producers;
	}

	/**
	 * Attempts to insert the relevant resource details in the database. The lastContactTime is always set to 'now', as
	 * this avoids any problems with a delayed replication message setting the time earlier.
	 */
	private void registerResource(RegistryResource resource, String metaTableName, boolean isMaster, int hrpSecs) throws RGMAPermanentException {
		StringBuilder insert = new StringBuilder("REPLACE INTO ");
		insert.append(metaTableName);
		insert.append(" values( '");
		insert.append(resource.getTableName());
		insert.append("', ");
		insert.append(resource.getEndpoint().getResourceID());
		insert.append(", '");
		insert.append(resource.getEndpoint().getURL());
		insert.append("' ,\"");
		insert.append(resource.getPredicate());
		insert.append("\", ");
		char master = 'N';
		int tIntervalSecs = resource.getTerminationIntervalSecs();
		if (isMaster) {
			master = 'Y';
		} else {
			master = 'N';
			/*
			 * this can only be a registration from a replica message so add the lag to compensate for network/system
			 * lag
			 */
			tIntervalSecs += m_registry_replication_lag;
		}

		insert.append(tIntervalSecs);
		insert.append(", '");

		if (resource.isContinuous()) {
			insert.append("Y', '");
		} else {
			insert.append("N', '");
		}

		if (resource.isHistory()) {
			insert.append("Y', '");
		} else {
			insert.append("N', '");
		}

		if (resource.isLatest()) {
			insert.append("Y', '");
		} else {
			insert.append("N', '");
		}

		if (resource.isStatic()) {
			insert.append("Y', '");
		} else {
			insert.append("N', '");
		}

		if (resource.isSecondary()) {
			insert.append("Y', ");
		} else {
			insert.append("N', ");
		}
		if (metaTableName.equals(m_producersTableName)) {
			insert.append(hrpSecs);
			insert.append(",");
		}
		insert.append(" '");
		insert.append(master);
		insert.append("', ");
		insert.append(System.currentTimeMillis() / 1000);
		insert.append(")");
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			con.executeUpdate(insert.toString());
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public Set<UnregisterConsumerResourceRequest> getConsumersFromRemote(String hostName) throws RGMAPermanentException {
		MySQLConnection con = null;
		Set<UnregisterConsumerResourceRequest> result = new HashSet<UnregisterConsumerResourceRequest>();
		try {
			con = new MySQLConnection();
			ResultSet rs = con.executeQuery("SELECT ID, URL FROM " + m_consumersTableName + " WHERE URL LIKE '%//" + hostName + ":%'");
			while (rs.next()) {
				ResourceEndpoint ep = new ResourceEndpoint(new URL(rs.getString("URL")), rs.getInt("ID"));
				result.add(new UnregisterConsumerResourceRequest(ep));
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return result;
	}

	public Set<UnregisterProducerTableRequest> getProducersFromRemote(String hostName) throws RGMAPermanentException {
		MySQLConnection con = null;
		Set<UnregisterProducerTableRequest> result = new HashSet<UnregisterProducerTableRequest>();
		try {
			con = new MySQLConnection();
			ResultSet rs = con.executeQuery("SELECT tableName, ID, URL FROM " + m_producersTableName + " WHERE URL LIKE '%//" + hostName + ":%'");
			while (rs.next()) {
				ResourceEndpoint ep = new ResourceEndpoint(new URL(rs.getString("URL")), rs.getInt("ID"));
				result.add(new UnregisterProducerTableRequest(rs.getString("tableName"), ep));
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return result;
	}
}