/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.schema;

import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.glite.rgma.server.services.database.MySQLConnection;
import org.glite.rgma.server.services.sql.ColumnDefinition;
import org.glite.rgma.server.services.sql.CreateIndexStatement;
import org.glite.rgma.server.services.sql.CreateTableStatement;
import org.glite.rgma.server.services.sql.CreateViewStatement;
import org.glite.rgma.server.services.sql.DataType;
import org.glite.rgma.server.services.sql.Expression;
import org.glite.rgma.server.services.sql.SQLExpEvaluator;
import org.glite.rgma.server.services.sql.Tuple;
import org.glite.rgma.server.services.sql.DataType.Type;
import org.glite.rgma.server.services.sql.SQLExpEvaluator.NullFound;
import org.glite.rgma.server.services.sql.Tuple.UnknownAttribute;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.SchemaColumnDefinition;
import org.glite.rgma.server.system.SchemaIndex;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UserContextInterface;

/*
 * Note that all public methods are synchronized expept for those that are read only and only make
 * one read operation on the database
 */
class MySQLSchemaDatabase implements SchemaDatabase {

	private class MetaData {

		/** Does the value new quotes */
		public List<Boolean> m_string;

		/** Number of columns */
		public int width;
	}

	private final static int NUMBER_OF_METADATA_COLUMNS = 4;

	private static final DataType I32 = new DataType();

	private static final DataType I64 = new DataType();

	private static final Pattern s_attTypePattern = Pattern.compile("^\\s*(\\w+)\\s*(?:\\(\\s*(\\d+)\\s*\\)\\s*)?$");

	static {
		I32.setType(Type.INTEGER);
		I64.setType(Type.BIGINT);
	}

	private int m_columnsNext;

	private final String m_columns;

	private int m_indexToColumnsNext;

	private final String m_indexToColumns;

	private int m_indicesNext;

	private final String m_indices;

	private long m_masterTime;

	private final String m_masterTimeTableName;

	private Map<String, MetaData> m_metadata = new HashMap<String, MetaData>();

	private int m_tableRulesNext;

	private final String m_tableRules;

	private int m_tablesNext;

	private final String m_tables;

	private String m_vdbNameUpper;

	private int m_viewRulesNext;

	private final String m_viewRules;

	private int m_viewsNext;

	private final String m_views;

	private int m_viewToColumnsNext;

	private final String m_viewToColumns;

	private boolean m_isMaster;

	private final static String[] s_tableSuffices = { "Tables", "TableRules", "Columns", "Views", "ViewToColumns", "ViewRules", "Indices", "IndexToColumns",
			"MasterTime" };
	private String[] m_fullNames = new String[s_tableSuffices.length];

	private List<String> m_vdbRules;

	/**
	 * Creates a new MySQLRegistryDatabase object.
	 * 
	 * @param rules
	 */
	MySQLSchemaDatabase(String vdbName, URL masterURL, URL serviceURL, List<String> rules) throws RGMAPermanentException {
		m_vdbRules = rules;
		m_isMaster = masterURL.equals(serviceURL);
		m_vdbNameUpper = vdbName;
		String prefix = m_vdbNameUpper + "_Schema_";
		int i = 0;
		for (String s : s_tableSuffices) {
			m_fullNames[i++] = prefix + s;
		}

		m_tables = m_fullNames[0];
		m_tableRules = m_fullNames[1];
		m_columns = m_fullNames[2];
		m_views = m_fullNames[3];
		m_viewToColumns = m_fullNames[4];
		m_viewRules = m_fullNames[5];
		m_indices = m_fullNames[6];
		m_indexToColumns = m_fullNames[7];
		m_masterTimeTableName = m_fullNames[8];

		MySQLConnection con = null;
		try {
			MySQLConnection.init();
			con = new MySQLConnection();
			java.sql.ResultSet rs = con.executeQuery("SHOW TABLES LIKE '" + prefix + "%'");
			int n = 0;
			int nExpected = 9;
			while (rs.next()) {
				n++;
			}
			if (n == 0) {
				createSchemaDatabaseTables(con, masterURL);
			} else if (n != nExpected) {
				throw new RGMAPermanentException("There are only " + n + " of the expected " + nExpected + " tables in the schema");
			} else { /*
					 * Modify old tables to eliminate the not null on the predicate for table rules. TODO remove when no
					 * longer needed
					 */
				con.executeUpdate("ALTER TABLE " + m_tableRules + " MODIFY COLUMN predicate TEXT");
			}

			refreshCache(con);
			buildMetadata(con, m_tables);
			buildMetadata(con, m_columns);
			buildMetadata(con, m_tableRules);
			buildMetadata(con, m_views);
			buildMetadata(con, m_viewToColumns);
			buildMetadata(con, m_viewRules);
			buildMetadata(con, m_indices);
			buildMetadata(con, m_indexToColumns);
			buildMetadata(con, m_masterTimeTableName);
			/*
			 * The master has a timestamp of 'now' so that any request will trigger a partial update message even if it
			 * only contains the master table. The slaves however have a timestamp of 0 to ensure that they get a
			 * partial update from the master.
			 */
			if (m_isMaster) {
				updateMasterTime(con, System.currentTimeMillis());
			} else {
				updateMasterTime(con, 0L);
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public synchronized boolean alter(String torv, String tableName, String action, String name, String type, UserContextInterface requestContext)
			throws RGMAPermanentException, RGMAPermanentException {
		long now = System.currentTimeMillis();
		checkUpdateTimestampValid(now);
		SchemaTableDefinition existing = getTableDefinition(tableName, null);

		torv = torv.toUpperCase();
		action = action.toUpperCase();

		boolean table = torv.toUpperCase().equals("TABLE");
		boolean view = torv.toUpperCase().equals("VIEW");
		boolean add = action.equals("ADD");
		boolean drop = action.equals("DROP");
		boolean modify = action.equals("MODIFY");

		/* Check and build a DataType */
		DataType dtype = null;
		if (type != null) {
			Matcher m = s_attTypePattern.matcher(type);
			if (!m.matches()) {
				throw new RGMAPermanentException("Type has bad format.");
			}
			String size = m.group(2);
			dtype = new DataType(DataType.Type.getType(m.group(1)), (size == null ? 0 : Integer.parseInt(size)));
		}

		/* Check parameters */
		if (!add && !drop && !modify)
			throw new RGMAPermanentException("Action must be add, modify or drop.");
		if (!table && !view)
			throw new RGMAPermanentException("Alter can only operate on a table or a view.");
		if (table && (add || modify) && type == null)
			throw new RGMAPermanentException("Add and modify require a type value.");

		SchemaColumnDefinition column = existing.getColumn(name);

		MySQLConnection con = null;
		try {
			con = new MySQLConnection();

			if (table) {

				if (existing.isView()) {
					throw new RGMAPermanentException("This is a view not a table.");
				}

				checkAuthz(con, tableName, requestContext, 'W');
				int tableID = getTableId(con, tableName);

				/* Find min and max column ID in the table */
				java.sql.ResultSet rs = con.executeQuery("SELECT MIN(ID), MAX(ID) FROM " + m_columns + " WHERE tableID=" + tableID);
				rs.first();
				int minID = rs.getInt(1);
				int maxID = rs.getInt(2);

				/* Do the add operations */
				if (add) { /* table add */
					if (column != null) {
						if (column.getType().equals(dtype)) {
							return false;
						} else {
							throw new RGMAPermanentException("Column already exists with type '" + column.getType() + "'.");
						}
					}
					int increment = m_columnsNext - minID;

					/* Update timestamps before changing the other tables and invalidating the selection condition */
					con.executeUpdate("UPDATE " + m_views + " SET unixTime = " + now + " WHERE ID IN (SELECT vc.viewID FROM " + m_columns + " c, "
							+ m_viewToColumns + " vc WHERE c.ID BETWEEN " + minID + " AND " + maxID + " AND c.ID = vc.columnID)");
					con.executeUpdate("UPDATE " + m_indices + " SET unixTime = " + now + " WHERE ID IN (SELECT ic.indexID FROM " + m_columns + " c, "
							+ m_indexToColumns + " ic WHERE c.ID BETWEEN " + minID + " AND " + maxID + " AND c.ID = ic.columnID)");

					/* Update columns */
					con.executeUpdate("UPDATE " + m_columns + " SET ID = ID + " + increment + " WHERE ID BETWEEN " + minID + " AND "
							+ (maxID - NUMBER_OF_METADATA_COLUMNS));
					con.executeUpdate("UPDATE " + m_columns + " SET ID = ID + " + (increment + 1) + " WHERE ID BETWEEN "
							+ (maxID - NUMBER_OF_METADATA_COLUMNS + 1) + " AND " + maxID);

					/* Update view to columns */
					con.executeUpdate("UPDATE " + m_viewToColumns + " SET columnID = columnID + " + increment + " WHERE columnID BETWEEN " + minID + " AND "
							+ (maxID - NUMBER_OF_METADATA_COLUMNS));
					con.executeUpdate("UPDATE " + m_viewToColumns + " SET columnID = columnID + " + (increment + 1) + " WHERE columnID BETWEEN "
							+ (maxID - NUMBER_OF_METADATA_COLUMNS + 1) + " AND " + maxID);

					/* Update index to columns */
					con.executeUpdate("UPDATE " + m_indexToColumns + " SET columnID = columnID + " + (increment + 1) + " WHERE columnID BETWEEN "
							+ (maxID - NUMBER_OF_METADATA_COLUMNS + 1) + " AND " + maxID);

					con.executeUpdate("UPDATE " + m_indexToColumns + " SET columnID = columnID + " + increment + " WHERE columnID BETWEEN " + minID + " AND "
							+ (maxID - NUMBER_OF_METADATA_COLUMNS));

					/* Add the new column */
					con.executeUpdate("INSERT INTO " + m_columns + " VALUES " + "(" + (m_columnsNext + maxID - minID + 1 - NUMBER_OF_METADATA_COLUMNS) + ", "
							+ "'" + name + "', " + tableID + ", '" + dtype.getType().toString() + "', " + dtype.getSize() + ", 'false', 'false')");
					m_columnsNext = m_columnsNext + maxID - minID + 2;
					con.executeUpdate("UPDATE " + m_tables + " SET numOfUserColumns = numOfUserColumns + 1 WHERE ID = " + tableID);

				} else if (drop) { /* table drop */
					if (column == null) {
						return false;
					} else {
						/* Check for NOT NULL or presence in index or view */
						if (column.isNotNull()) {
							throw new RGMAPermanentException("Columns that are part of the primary key or are 'NOT NULL' may not be dropped.");
						}
						rs = con.executeQuery("SELECT i.name FROM " + m_indexToColumns + " ic, " + m_indices + " i, " + m_columns
								+ " c WHERE columnID BETWEEN " + minID + " AND " + maxID + " AND ic.indexID = i.ID AND ic.columnID = c.ID AND c.name='"
								+ column.getName() + "'");
						if (rs.next()) {
							throw new RGMAPermanentException("Index '" + rs.getString(1) + "' must be dropped before the column can be dropped.");
						}
						rs = con.executeQuery("SELECT v.name FROM " + m_viewToColumns + " vc, " + m_views + " v, " + m_columns + " c WHERE columnID BETWEEN "
								+ minID + " AND " + maxID + " AND vc.viewID = v.ID AND vc.columnID = c.ID AND c.name='" + column.getName() + "'");
						if (rs.next()) {
							throw new RGMAPermanentException("View '" + rs.getString(1) + "' must be dropped before the column can be dropped.");
						}

						/* Delete the column" */
						con.executeUpdate("DELETE FROM " + m_columns + " WHERE tableID=" + tableID + " AND name='" + column.getName() + "'");
						con.executeUpdate("UPDATE " + m_tables + " SET numOfUserColumns = numOfUserColumns - 1 WHERE ID = " + tableID);

					}
				} else { /* table modify */
					if (column == null) {
						throw new RGMAPermanentException("Column '" + name + "' is not in the table.");
					}
					if (dtype.equals(column.getType())) {
						return false;
					}
					if (column.getType().getType() != DataType.Type.CHAR && column.getType().getType() != DataType.Type.VARCHAR) {
						throw new RGMAPermanentException("You may not convert type from '" + column.getType() + "' to '" + dtype + "'.");
					}
					if (dtype.getType() != DataType.Type.CHAR && dtype.getType() != DataType.Type.VARCHAR) {
						throw new RGMAPermanentException("You may not convert type from '" + column.getType() + "' to '" + dtype + "'.");
					}
					if (dtype.getSize() < column.getType().getSize()) {
						throw new RGMAPermanentException("You may not convert type from '" + column.getType() + "' to '" + dtype + "'.");
					}
					con.executeUpdate("UPDATE " + m_columns + " SET SQLType = '" + dtype.getType().toString() + "', size = " + dtype.getSize()
							+ " WHERE ID BETWEEN " + minID + " AND " + maxID + " AND name = '" + column.getName() + "'");
				}

				/* Update the base table timestamp */
				con.executeUpdate("UPDATE " + m_tables + " SET unixTime = " + now + " WHERE ID = " + tableID);

			} else { /* view */

				if (!existing.isView()) {
					throw new RGMAPermanentException("This is a table not a view.");
				}

				String viewName = tableName;
				tableName = existing.getViewFor();
				checkAuthz(con, tableName, requestContext, 'W');
				int tableID = getTableId(con, tableName);

				/* Find min and max column ID in the table */
				java.sql.ResultSet rs = con.executeQuery("SELECT MIN(vc.ID), MAX(vc.ID), vc.viewID FROM " + m_viewToColumns + " vc, " + m_views
						+ " v WHERE vc.viewID = v.ID AND v.name = '" + viewName + "'");
				rs.first();
				int minID = rs.getInt(1);
				int maxID = rs.getInt(2);
				int viewID = rs.getInt(3);

				/* Do the add operations */
				if (add) { /* view add */
					if (column != null) {
						return false;
					}

					int increment = m_viewToColumnsNext - minID;
					con.executeUpdate("UPDATE " + m_viewToColumns + " SET ID = ID + " + increment + " WHERE ID BETWEEN " + minID + " AND " + maxID);

					rs = con.executeQuery("SELECT ID FROM " + m_columns + " WHERE name = '" + name + "' AND tableID =" + tableID);
					if (!rs.next()) {
						throw new RGMAPermanentException("Column '" + name + "' is not in the table.");
					}

					int columnID = rs.getInt(1);

					con.executeUpdate("INSERT INTO " + m_viewToColumns + " VALUES (" + (m_viewToColumnsNext + maxID - minID + 1) + "," + viewID + ", "
							+ columnID + ")");
					m_viewToColumnsNext = m_viewToColumnsNext + maxID - minID + 2;

				} else if (drop) {/* view drop */
					if (column == null) {
						return false;
					}

					if (existing.getColumns().size() <= 1) {
						throw new RGMAPermanentException("A view must contain at least one column.");
					}

					/* Delete the column" */
					con.executeUpdate("DELETE FROM " + m_viewToColumns + " WHERE viewID=" + viewID + " AND columnID IN (SELECT ID FROM " + m_columns
							+ " WHERE name = '" + name + "')");

				} else { /* view modify */
					throw new RGMAPermanentException("Type of column in a view may not be modified.");
				}

				/* Update the base table timestamp */
				con.executeUpdate("UPDATE " + m_views + " SET unixTime = " + now + " WHERE ID = " + viewID);

			}

			/* Update master table timestamp */
			updateMasterTime(con, now);

		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return true;
	}

	public synchronized boolean createIndex(String createIndexStatement, UserContextInterface requestContext) throws RGMAPermanentException,
			RGMAPermanentException, RGMAPermanentException {
		long now = System.currentTimeMillis();
		checkUpdateTimestampValid(now);
		CreateIndexStatement cis = null;
		try {
			cis = CreateIndexStatement.parse(createIndexStatement);
		} catch (ParseException e) {
			throw new RGMAPermanentException("Parse exception in create index statement: " + e.getMessage());
		}
		String tableName = cis.getTableName();
		String indexName = cis.getIndexName();
		List<String> columnNames = new ArrayList<String>();
		for (String column : cis.getColumnNames()) {
			columnNames.add(column.toUpperCase());
		}
		MySQLConnection con = null;

		try {
			con = new MySQLConnection();
			checkAuthz(con, tableName, requestContext, 'W');
			int tableID = getTableId(con, tableName);
			String getIndexName = "SELECT * FROM " + m_indices + " WHERE name = '" + indexName + "' AND isSkeleton = 'false' AND tableID = " + tableID;
			java.sql.ResultSet rs = con.executeQuery(getIndexName);
			if (rs.next()) {
				String schemaCreateStatement = getIndexDesc(con, indexName, tableName);
				CreateIndexStatement cisFromSchema = CreateIndexStatement.parse(schemaCreateStatement);
				if (cisFromSchema.equals(cis)) {
					return false;
				} else {
					throw new RGMAPermanentException("Index already exists with a different definition.");
				}
			} else {
				int tableId = getTableId(con, tableName);
				List<Integer> columnsIDs = new ArrayList<Integer>();
				for (String colName : columnNames) {
					String getColumnID = "SELECT ID FROM " + m_columns + " WHERE name = '" + colName + "' AND tableID =" + tableId;
					rs = con.executeQuery(getColumnID);
					if (!rs.next()) {
						throw new RGMAPermanentException("Column '" + colName + "' is not in the table.");
					}
					int colId = rs.getInt("ID");
					if (columnsIDs.contains(colId)) {
						throw new RGMAPermanentException("Duplicate column names in index definition");
					}
					columnsIDs.add(colId);
				}
				String updateAnIndexName = "UPDATE " + m_indices + " SET isSkeleton = 'false', unixTime = " + now + " WHERE name = '" + indexName
						+ "' AND tableID = " + tableID;
				int indexId;
				if (con.executeUpdate(updateAnIndexName) == 0) {
					indexId = m_indicesNext;
					String addAnIndexName = "INSERT INTO " + m_indices + " VALUES (" + indexId + ", '" + indexName + "', '" + tableID + "', 'false', " + now
							+ ")";
					con.executeUpdate(addAnIndexName);
					m_indicesNext++;
				} else {
					String getIndexId = "SELECT ID from " + m_indices + " WHERE name= '" + indexName + "' AND tableID =" + tableId;
					rs = con.executeQuery(getIndexId);
					rs.first();
					indexId = rs.getInt("ID");
				}
				for (int columnId : columnsIDs) {
					String addColumn = "INSERT INTO " + m_indexToColumns + " VALUES (" + m_indexToColumnsNext + "," + indexId + "," + columnId + ")";
					con.executeUpdate(addColumn);
					m_indexToColumnsNext++;
				}

				/* Update its base table timestamp */
				String updateBaseTable = "UPDATE " + m_tables + " SET unixTime = " + now + " WHERE ID = " + tableID;
				con.executeUpdate(updateBaseTable);

				/* Update master table timestamp */
				updateMasterTime(con, now);

			}
		} catch (ParseException pe) {
			throw new RGMAPermanentException("Cannot parse: " + createIndexStatement);
		} catch (SQLException e) {
			throw new RGMAPermanentException("Problem encountered extracting from result set", e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return true;
	}

	/**
	 * Creates a new table definition in the Schema. CREATE TABLE statement is verified and then inserted into Schema DB
	 * if necessary. Syntax for CREATE: CREATE TABLE name ( field_name field_type [modifiers], ... ) modifiers := NOT
	 * NULL, PRIMARY KEY
	 * 
	 * @param createStatement
	 *            the CREATE TABLE statement.
	 */
	public synchronized boolean createTable(String createStatement, List<String> tableAuthz, UserContextInterface requestContext)
			throws RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		String incomeDN = requestContext.getDN();
		long now = System.currentTimeMillis();
		checkUpdateTimestampValid(now);
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			CreateTableStatement cts = CreateTableStatement.parse(createStatement);
			List<ColumnDefinition> columns = cts.getColumns();
			appendMetadataColumns(cts);
			List<String> ctsColumnNames = new ArrayList<String>();
			for (ColumnDefinition ctsColumn : columns) {
				ctsColumnNames.add(ctsColumn.getName().toUpperCase());
			}
			boolean schemaRuleFound = Authz.checkRules(tableAuthz, ctsColumnNames, Authz.RuleApplicability.TABLE);

			if (!schemaRuleFound) { /* Add RW rule for creator if no other schema rule defined */
				tableAuthz = new ArrayList<String>(tableAuthz);
				tableAuthz.add("[DN]='" + requestContext.getDN() + "':RW");
			}

			String tableName = cts.getTableName();
			checkAuthz(con, tableName, requestContext, 'C');

			/*
			 * Check that there are no columns that are NOT NULL except PRIMARY KEY, that there is a PRIMARY KEY and
			 * that there are no duplicate columns.
			 */
			boolean foundAPrimaryKey = false;
			for (int index1 = 0; index1 < columns.size() - NUMBER_OF_METADATA_COLUMNS; index1++) {
				ColumnDefinition colDef = columns.get(index1);
				String colNameUpper = colDef.getName().toUpperCase();
				for (int index2 = index1 + 1; index2 < columns.size() - NUMBER_OF_METADATA_COLUMNS; index2++) {
					if (colNameUpper.equals(columns.get(index2).getName().toUpperCase())) {
						throw new RGMAPermanentException("Duplicate (case-insensitive) column names found: '" + colNameUpper + "'.");
					}
				}
				if (colDef.isPrimaryKey()) {
					foundAPrimaryKey = true;
				} else {
					if (colDef.isNotNull()) {
						throw new RGMAPermanentException("Only PRIMARY KEY components may be declared to be NOT NULL: '" + colNameUpper + "'.");
					}
				}
			}
			if (!foundAPrimaryKey) {
				throw new RGMAPermanentException("A PRIMARY KEY must be defined.");
			}

			if (tableExists(tableName)) {
				String schemaCreateStatement = getTableDesc(con, tableName);
				try {
					CreateTableStatement ctsFromSchema = CreateTableStatement.parse(schemaCreateStatement);
					appendMetadataColumns(ctsFromSchema);
					if (ctsFromSchema.equals(cts)) {
						if (!isRulesSetMatched(tableName, tableAuthz)) {
							throw new RGMAPermanentException("Table already exists but with a different set of authz rules.");
						}
						return false;
					} else {
						throw new RGMAPermanentException("Table already exists with a different definition.");
					}
				} catch (ParseException e) {
					throw new RGMAPermanentException(e);
				}
			} else {
				if (viewExists(tableName)) {
					throw new RGMAPermanentException(createStatement + "The requested table name is already in use as a view name.");
				}

				/* Create new table */
				int uColNum = cts.getColumns().size() - NUMBER_OF_METADATA_COLUMNS;
				String sqlUpdateTable = "UPDATE " + m_tables + " SET numOfUserColumns = " + uColNum + ", isSkeleton = 'false', DN = '" + incomeDN
						+ "', unixTime =" + now + " WHERE name = '" + tableName + "'";

				int tableId;
				if (con.executeUpdate(sqlUpdateTable) == 0) {
					// If there is no table skeleton for update, create a new one
					tableId = m_tablesNext;
					String sqlAddTable = "INSERT INTO " + m_tables + " VALUES" + "(" + tableId + ", '" + tableName + "', " + uColNum + ", 'false', '"
							+ incomeDN + "', " + now + ")";
					con.executeUpdate(sqlAddTable);
					m_tablesNext++;
				} else {
					tableId = getTableId(con, tableName);
				}

				for (ColumnDefinition cd : cts.getColumns()) {
					String isNotNull = cd.isNotNull() ? "true" : "false";
					String isPrimaryKey = cd.isPrimaryKey() ? "true" : "false";
					String sqlAddAColumn = "INSERT INTO " + m_columns + " VALUES " + "(" + m_columnsNext + ", " + "'" + cd.getName() + "', " + tableId + ", '"
							+ cd.getType().getType().toString() + "', " + cd.getType().getSize() + ", '" + isNotNull + "'," + "'" + isPrimaryKey + "')";
					con.executeUpdate(sqlAddAColumn);
					m_columnsNext++;
				}

				// Add all rules one by one. Syntax has already been checked.
				for (String rule : tableAuthz) {

					String predicate;
					String credential;
					String action;

					Matcher m = Authz.s_dataPattern.matcher(rule);
					if (m.matches()) {
						predicate = "'" + m.group(1).replaceAll("'", "\\\\'") + "'";
						credential = m.group(2).replaceAll("'", "\\\\'");
						action = m.group(3).toUpperCase();
					} else {
						m = Authz.s_schemaPattern.matcher(rule);
						m.matches();
						predicate = "null";
						credential = m.group(1).replaceAll("'", "\\\\'");
						action = m.group(2).toUpperCase();
					}

					String isReadable = action.indexOf('R') >= 0 ? "true" : "false";
					String isWritable = action.indexOf('W') >= 0 ? "true" : "false";
					con.executeUpdate("INSERT INTO " + m_tableRules + " VALUES " + "(" + m_tableRulesNext + "," + tableId + ", " + predicate + ", '"
							+ credential + "'," + "'" + isReadable + "','" + isWritable + "')");
					m_tableRulesNext++;
				}

				/* Update master table timestamp */
				updateMasterTime(con, now);
			}
		} catch (ParseException e) {
			throw new RGMAPermanentException("Unable to parse create table statement: " + e.getMessage());
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return true;
	}

	public synchronized boolean createView(String createViewStatement, List<String> viewRules, UserContextInterface requestContext)
			throws RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		long now = System.currentTimeMillis();
		checkUpdateTimestampValid(now);
		CreateViewStatement cvs = null;
		try {
			cvs = CreateViewStatement.parse(createViewStatement);
		} catch (ParseException e) {
			throw new RGMAPermanentException("Parse exception in create view statement: " + e.getMessage());
		}
		String tableName = cvs.getTableName();
		String viewName = cvs.getViewName();
		List<String> columnNames = new ArrayList<String>();
		for (String column : cvs.getColumnNames()) {
			columnNames.add(column.toUpperCase());
		}
		MySQLConnection con = null;

		try {
			con = new MySQLConnection();
			checkAuthz(con, tableName, requestContext, 'W');
			Authz.checkRules(viewRules, columnNames, Authz.RuleApplicability.VIEW);

			if (viewExists(viewName)) {
				String schemaCreateStatement = getViewDesc(con, viewName);
				CreateViewStatement cvsFromSchema;
				try {
					cvsFromSchema = CreateViewStatement.parse(schemaCreateStatement);
				} catch (ParseException e) {
					throw new RGMAPermanentException("There are errors in the generated CreateStatement " + schemaCreateStatement, e);
				}
				if (cvsFromSchema.equals(cvs)) {
					if (!isRulesSetMatched(viewName, viewRules)) {
						throw new RGMAPermanentException("View already exists but with a different set of authz rules.");
					}
					return false;
				} else {
					throw new RGMAPermanentException("View already exists with a different definition.");
				}
			} else {
				int tableId = getTableId(con, tableName);
				List<Integer> columnsIDs = new ArrayList<Integer>();
				for (String colName : columnNames) {
					String getColumnID = "SELECT ID FROM " + m_columns + " WHERE name = '" + colName + "' AND tableID =" + tableId;
					java.sql.ResultSet columnIDResult = con.executeQuery(getColumnID);
					if (!columnIDResult.next()) {
						throw new RGMAPermanentException("Column '" + colName + "' name is not in the table");
					}
					int colId = columnIDResult.getInt("ID");
					if (columnsIDs.contains(colId)) {
						throw new RGMAPermanentException("Duplicate column names in view definition");
					}
					columnsIDs.add(colId);
				}
				String updateView = "UPDATE " + m_views + " SET isSkeleton = 'false', unixTime =" + now + " WHERE name ='" + viewName + "' AND tableID = "
						+ tableId;
				int viewId;
				if (con.executeUpdate(updateView) == 0) {
					viewId = m_viewsNext;
					String addView = "INSERT INTO " + m_views + " VALUES (" + viewId + ",'" + viewName + "', '" + tableId + "', 'false', " + now + ")";
					con.executeUpdate(addView);
					m_viewsNext++;
				} else {
					viewId = getViewId(con, viewName);
				}
				for (int columnId : columnsIDs) {
					String addAViewColumn = "INSERT INTO " + m_viewToColumns + " VALUES (" + m_viewToColumnsNext + "," + viewId + ",'" + columnId + "')";
					con.executeUpdate(addAViewColumn);
					m_viewToColumnsNext++;
				}

				// Add all rules one by one. Syntax has already been checked.
				for (String rule : viewRules) {

					Matcher m = Authz.s_dataPattern.matcher(rule);
					m.matches();
					String predicate = m.group(1).replaceAll("'", "\\\\'");
					String credential = m.group(2).replaceAll("'", "\\\\'");

					String sqlAddARule = "INSERT INTO " + m_viewRules + " VALUES " + "(" + m_viewRulesNext + "," + viewId + ", '" + predicate + "', '"
							+ credential + "')";
					con.executeUpdate(sqlAddARule);
					m_viewRulesNext++;
				}

				// Update its base table timestamp
				String updateBaseTable = "UPDATE " + m_tables + " SET unixTime = " + now + " WHERE ID = " + tableId;
				con.executeUpdate(updateBaseTable);

				/* Update master table timestamp */
				updateMasterTime(con, now);
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return true;
	}

	public synchronized boolean dropIndex(String tableName, String indexName, UserContextInterface requestContext) throws RGMAPermanentException,
			RGMAPermanentException, RGMAPermanentException {
		long now = System.currentTimeMillis();
		checkUpdateTimestampValid(now);
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			checkAuthz(con, tableName, requestContext, 'W');

			String getIndexID = "SELECT i.ID FROM " + m_tables + " t," + m_indices + " i WHERE i.isSkeleton = 'false' AND t.name = '" + tableName
					+ "' AND i.name = '" + indexName + "'";
			java.sql.ResultSet rs = con.executeQuery(getIndexID);
			if (!rs.next()) {
				return false;
			}
			int indexID = rs.getInt("i.ID");

			dropIndexBits(con, indexID);
			con.executeUpdate("UPDATE " + m_indices + " SET unixTime = " + now + ", isSkeleton = 'true' WHERE ID = " + indexID);

			/* Update master table timestamp */
			updateMasterTime(con, now);
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return true;
	}

	public synchronized boolean dropTable(String tableName, UserContextInterface requestContext) throws RGMAPermanentException, RGMAPermanentException,
			RGMAPermanentException {
		long now = System.currentTimeMillis();
		checkUpdateTimestampValid(now);
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			int tableID = getTableId(con, tableName);
			if (tableID == 0) {
				return false;
			}
			checkAuthz(con, tableName, requestContext, 'W');

			java.sql.ResultSet rs = con.executeQuery("SELECT ID FROM " + m_views + " WHERE tableID =" + tableID);
			while (rs.next()) {
				int viewID = rs.getInt("ID");
				dropViewBits(con, viewID);
				con.executeUpdate("UPDATE " + m_views + " SET unixTime = " + now + ", isSkeleton = 'true' WHERE ID =" + viewID);
			}

			rs = con.executeQuery("SELECT ID FROM " + m_indices + " WHERE tableID =" + tableID);
			while (rs.next()) {
				int indexID = rs.getInt("ID");
				dropIndexBits(con, indexID);
				con.executeUpdate("UPDATE " + m_indices + " SET unixTime = " + now + ", isSkeleton = 'true' WHERE ID =" + indexID);
			}

			dropTableBits(con, tableID);
			con.executeUpdate("UPDATE " + m_tables + " SET unixTime = " + now + ", isSkeleton = 'true' WHERE ID = " + tableID);

			/* Update master table timestamp */
			updateMasterTime(con, now);
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return true;
	}

	public synchronized boolean dropView(String viewName, UserContextInterface requestContext) throws RGMAPermanentException, RGMAPermanentException,
			RGMAPermanentException {
		long now = System.currentTimeMillis();
		checkUpdateTimestampValid(now);
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			int viewId = getViewId(con, viewName);
			if (viewId == 0) {
				return false;
			}
			String getTableName = "SELECT t.name, v.ID FROM " + m_tables + " t," + m_views + " v WHERE t.ID = tableID and v.name ='" + viewName + "'";
			java.sql.ResultSet rs = con.executeQuery(getTableName);
			rs.first();
			String tableName = rs.getString("t.name");
			checkAuthz(con, tableName, requestContext, 'W');

			dropViewBits(con, viewId);
			con.executeUpdate("UPDATE " + m_views + " SET unixTime = " + now + ", isSkeleton = 'true' WHERE ID = " + viewId);

			/* Update master table timestamp */
			updateMasterTime(con, now);
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return true;
	}

	public synchronized List<TupleSet> getAllSchema() throws RGMAPermanentException {
		return getSchemaUpdates(0);
	}

	public synchronized List<String> getAllTables(UserContextInterface requestContext) throws RGMAPermanentException {
		List<String> m_tableName = new ArrayList<String>();
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			String getTableAndViewNames = "(SELECT name, 0 AS tableID FROM " + m_tables + " WHERE isSkeleton = 'false') UNION (SELECT name, tableID FROM "
					+ m_views + " WHERE isSkeleton = 'false') ORDER BY name";
			java.sql.ResultSet rs = con.executeQuery(getTableAndViewNames);
			while (rs.next()) {
				String torvName = rs.getString("name");
				int tableID = rs.getInt("tableID");
				String tableName;
				if (tableID != 0) { /* It's a view */
					ResultSet rs2 = con.executeQuery("SELECT name FROM " + m_tables + " WHERE ID = " + tableID);
					rs2.first();
					tableName = rs2.getString("name");
				} else {
					tableName = torvName;
				}
				try {
					checkAuthz(con, tableName, requestContext, 'R');
					m_tableName.add(torvName);
				} catch (RGMAPermanentException e) {
					/* Silent rejection */
				}
			}
			return m_tableName;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public synchronized List<String> getAuthorizationRules(String tableName, UserContextInterface requestContext) throws RGMAPermanentException,
			RGMAPermanentException {
		List<String> authzRules = new ArrayList<String>();
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			int tableId = getTableId(con, tableName);
			if (tableId != 0) {
				checkAuthz(con, tableName, requestContext, 'R');
				String tableAllRules = "SELECT predicate, credentials, isReadable, isWritable FROM " + m_tableRules + " WHERE tableID = " + tableId
						+ " ORDER BY ID";
				java.sql.ResultSet rs = con.executeQuery(tableAllRules);
				while (rs.next()) {
					StringBuilder arule = new StringBuilder();
					String predicate = rs.getString("predicate"); /* This may be a schema rule - without predicate */
					if (!rs.wasNull()) {
						arule.append(predicate).append(':');
					}
					arule.append(rs.getString("credentials")).append(':');
					if (rs.getString("isReadable").equals("true")) {
						arule.append('R');
					}
					if (rs.getString("isWritable").equals("true")) {
						arule.append('W');
					}
					authzRules.add(arule.toString());
				}
			} else {
				int viewId = getViewId(con, tableName);
				if (viewId != 0) {
					String viewAllRules = "SELECT predicate, credentials FROM " + m_viewRules + " WHERE viewID = " + viewId + " ORDER BY ID";
					java.sql.ResultSet rs = con.executeQuery(viewAllRules);
					while (rs.next()) {
						StringBuilder arule = new StringBuilder();
						arule.append(rs.getString("predicate")).append(':');
						arule.append(rs.getString("credentials")).append(':');
						arule.append('R');
						authzRules.add(arule.toString());
					}
				} else {
					throw new RGMAPermanentException("Table or view '" + m_vdbNameUpper + "." + tableName + "' not in schema.");
				}
			}
			return authzRules;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public long getMasterTime() throws RGMAPermanentException {
		return m_masterTime;
	}

	public synchronized List<TupleSet> getSchemaUpdates(long time) throws RGMAPermanentException {
		/* Check if a new update has happened since last update. If not, return an empty list. */
		List<TupleSet> rsList = new ArrayList<TupleSet>();
		if (time == m_masterTime) {
			return rsList;
		}
		/*
		 * If we have not passed the master time then changes could be missed. One ms delay should be enough but, just
		 * in case the hardware clock has been updated, use pauses of 1 second.
		 */
		while (System.currentTimeMillis() <= m_masterTime) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				/* Do nothing */;
			}
		}
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			addToResultSetList(con, rsList, m_tables, "WHERE unixTime > " + time + " ORDER BY ID");
			addToResultSetList(con, rsList, m_tableRules, "WHERE tableID IN (SELECT ID FROM " + m_tables + "  WHERE unixTime > " + time + ")" + " ORDER BY ID");
			addToResultSetList(con, rsList, m_columns, "WHERE tableID IN (SELECT ID FROM " + m_tables + "  WHERE unixTime > " + time + ")" + " ORDER BY ID");

			addToResultSetList(con, rsList, m_views, "WHERE unixTime > " + time + " ORDER BY ID");
			addToResultSetList(con, rsList, m_viewToColumns, "WHERE viewID IN (SELECT ID FROM " + m_views + "  WHERE unixTime > " + time + ")" + " ORDER BY ID");
			addToResultSetList(con, rsList, m_viewRules, "WHERE viewID IN (SELECT ID FROM " + m_views + "  WHERE unixTime > " + time + ")" + " ORDER BY ID");

			addToResultSetList(con, rsList, m_indices, "WHERE unixTime > " + time + " ORDER BY ID");
			addToResultSetList(con, rsList, m_indexToColumns, "WHERE indexID IN (SELECT ID FROM " + m_indices + "  WHERE unixTime > " + time + ")"
					+ " ORDER BY ID");

			/**
			 * This must be the last call to "addToResultSetList" to ensure that when slaves update, the master time
			 * stamp is only updated right at the end.
			 */
			addToResultSetList(con, rsList, m_masterTimeTableName, "");
			return rsList;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public synchronized SchemaTableDefinition getTableDefinition(String tableName, UserContextInterface requestContext) throws RGMAPermanentException,
			RGMAPermanentException {
		SchemaTableDefinition def = null;
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			int tableId = getTableId(con, tableName);
			if (tableId != 0) {
				checkAuthz(con, tableName, requestContext, 'R');
				String getColumns = "SELECT name, SQLType, size, isNotNull, isPrimaryKey FROM " + m_columns + " WHERE tableID = " + tableId + " ORDER BY ID";
				java.sql.ResultSet rs = con.executeQuery(getColumns);
				List<SchemaColumnDefinition> cols = new ArrayList<SchemaColumnDefinition>();
				while (rs.next()) {
					String colName = rs.getString("name");
					String colType = rs.getString("SQLType");
					int colSize = rs.getInt("size");
					boolean colIsNull = rs.getString("isNotNull").equals("true") ? true : false;
					boolean colIsPK = rs.getString("isPrimaryKey").equals("true") ? true : false;
					DataType dt = new DataType(Type.getType(colType), colSize);
					cols.add(new SchemaColumnDefinition(colName, dt, colIsNull, colIsPK));
				}
				def = new SchemaTableDefinition(tableName, cols);
			} else {
				String viewName = tableName;
				java.sql.ResultSet rs = con.executeQuery("SELECT ID, tableID FROM " + m_views + " WHERE isSkeleton = 'false' AND name = '" + viewName + "'");
				if (rs.next()) {
					int viewId = rs.getInt("ID");
					tableId = rs.getInt("tableID");
					rs = con.executeQuery("SELECT name FROM " + m_tables + " WHERE ID = " + tableId);
					rs.first();
					tableName = rs.getString("name");
					checkAuthz(con, tableName, requestContext, 'R');
					String getViewColumns = "SELECT name, SQLType, size, isNotNull, isPrimaryKey FROM " + m_columns + " c," + m_viewToColumns
							+ " WHERE columnID = c.ID and viewID = " + viewId + " ORDER BY " + m_viewToColumns + ".ID";
					rs = con.executeQuery(getViewColumns);
					List<SchemaColumnDefinition> cols = new ArrayList<SchemaColumnDefinition>();
					while (rs.next()) {
						String colName = rs.getString("name");
						String colType = rs.getString("SQLType");
						int colSize = rs.getInt("size");
						boolean colIsNull = rs.getString("isNotNull").equals("true") ? true : false;
						boolean colIsPK = rs.getString("isPrimaryKey").equals("true") ? true : false;
						DataType dt = new DataType(Type.getType(colType), colSize);
						cols.add(new SchemaColumnDefinition(colName, dt, colIsNull, colIsPK));
					}
					def = new SchemaTableDefinition(viewName, tableName, cols);
				} else {
					throw new RGMAPermanentException("Table or view '" + m_vdbNameUpper + "." + tableName + "' not in schema.");
				}
			}
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return def;
	}

	public synchronized List<SchemaIndex> getTableIndexes(String tableName, UserContextInterface requestContext) throws RGMAPermanentException,
			RGMAPermanentException {
		List<SchemaIndex> m_indexes = new ArrayList<SchemaIndex>();
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			int tableID = getTableId(con, tableName);
			if (tableID == 0) {
				throw new RGMAPermanentException("Table '" + m_vdbNameUpper + "." + tableName + "' not in schema.");
			}
			checkAuthz(con, tableName, requestContext, 'R');
			String getIndexName = "SELECT ID, name FROM " + m_indices + " WHERE isSkeleton = 'false' AND tableID = " + tableID;
			java.sql.ResultSet rs = con.executeQuery(getIndexName);
			while (rs.next()) {
				String indexName = rs.getString("name");
				int indexID = rs.getInt("ID");
				String getColumnNames = "SELECT name FROM " + m_columns + " c," + m_indexToColumns + " ic WHERE indexID =" + indexID
						+ " AND c.ID = ic.columnID ORDER BY ic.ID";
				java.sql.ResultSet rs2 = con.executeQuery(getColumnNames);
				List<String> m_indexColumnList = new ArrayList<String>();
				while (rs2.next()) {
					m_indexColumnList.add(rs2.getString("name"));
				}
				SchemaIndex m_index = new SchemaIndex(indexName, m_indexColumnList);
				m_indexes.add(m_index);
			}
			return m_indexes;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public long getTableTimestamp(String tableName) throws RGMAPermanentException {
		String getTableUpdateTime = "SELECT unixTime FROM " + m_tables + " WHERE name ='" + tableName + "'";
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			java.sql.ResultSet TableUpdateTimeResult = con.executeQuery(getTableUpdateTime);
			TableUpdateTimeResult.first();
			return TableUpdateTimeResult.getLong("unixTime");
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public synchronized void putAllSchema(List<TupleSet> rss) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			for (String tableName : m_metadata.keySet()) {
				con.executeUpdate("DELETE FROM " + tableName);
			}
			putSchemaUpdates(con, rss);
		} catch (NumberFormatException e) {
			throw new RGMAPermanentException(e);
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public synchronized void putSchemaUpdates(List<TupleSet> rss) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			putSchemaUpdates(con, rss);
		} catch (NumberFormatException e) {
			throw new RGMAPermanentException(e);
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public synchronized boolean setAuthorizationRules(String tableName, List<String> authzRules, UserContextInterface requestContext)
			throws RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		long now = System.currentTimeMillis();
		checkUpdateTimestampValid(now);
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			int tableId = getTableId(con, tableName);
			if (tableId != 0) {
				checkAuthz(con, tableName, requestContext, 'W');
				List<String> tableColumnNames = new ArrayList<String>();
				String getTableColumns = "SELECT name FROM " + m_columns + " WHERE tableID = " + tableId;
				java.sql.ResultSet rs = con.executeQuery(getTableColumns);
				while (rs.next()) {
					tableColumnNames.add(rs.getString("name").toUpperCase());
				}
				Authz.checkRules(authzRules, tableColumnNames, Authz.RuleApplicability.TABLE);

				/* Now do it */
				String deleteAuthzRules = "DELETE FROM " + m_tableRules + " WHERE tableID=" + tableId;
				con.executeUpdate(deleteAuthzRules);

				for (String rule : authzRules) {

					String predicate;
					String credential;
					String action;

					Matcher m = Authz.s_dataPattern.matcher(rule);
					if (m.matches()) {
						predicate = "'" + m.group(1).replaceAll("'", "\\\\'") + "'";
						credential = m.group(2).replaceAll("'", "\\\\'");
						action = m.group(3).toUpperCase();
					} else {
						m = Authz.s_schemaPattern.matcher(rule);
						m.matches();
						predicate = "null";
						credential = m.group(1).replaceAll("'", "\\\\'");
						action = m.group(2).toUpperCase();
					}

					String isReadable = action.indexOf('R') >= 0 ? "true" : "false";
					String isWritable = action.indexOf('W') >= 0 ? "true" : "false";
					String sqlAddARule = "INSERT INTO " + m_tableRules + " VALUES " + "(" + m_tableRulesNext + "," + tableId + ", " + predicate + ", '"
							+ credential + "'," + "'" + isReadable + "','" + isWritable + "')";
					con.executeUpdate(sqlAddARule);
					m_tableRulesNext++;
				}
				String setTableRulesUpdateTS = "UPDATE " + m_tables + " SET unixTime = " + now + " WHERE name ='" + tableName + "'";
				con.executeUpdate(setTableRulesUpdateTS);
			} else {
				String viewName = tableName;
				int viewId = getViewId(con, viewName);
				if (viewId != 0) {
					String getTableName = "SELECT t.name, v.ID FROM " + m_tables + " t," + m_views + " v WHERE t.ID = tableID and v.name ='" + viewName + "'";
					java.sql.ResultSet rs = con.executeQuery(getTableName);
					rs.first();
					tableName = rs.getString("t.name");
					checkAuthz(con, tableName, requestContext, 'W');
					List<String> tableColumnNames = new ArrayList<String>();
					String getTableColumns = "SELECT c.name FROM " + m_columns + " c, " + m_viewToColumns + " vc" + " WHERE viewID = " + viewId
							+ " AND c.ID = vc.columnID";
					rs = con.executeQuery(getTableColumns);
					while (rs.next()) {
						tableColumnNames.add(rs.getString("name").toUpperCase());
					}
					Authz.checkRules(authzRules, tableColumnNames, Authz.RuleApplicability.VIEW);

					/* Now do it */
					String deleteAuthzRules = "DELETE FROM " + m_viewRules + " WHERE viewID=" + viewId;
					con.executeUpdate(deleteAuthzRules);

					for (String rule : authzRules) {

						Matcher m = Authz.s_dataPattern.matcher(rule);
						m.matches();
						String predicate = m.group(1).replaceAll("'", "\\\\'");
						String credential = m.group(2).replaceAll("'", "\\\\'");

						String sqlAddARule = "INSERT INTO " + m_viewRules + " VALUES " + "(" + m_viewRulesNext + "," + viewId + ", '" + predicate + "', '"
								+ credential + "')";
						con.executeUpdate(sqlAddARule);
						m_viewRulesNext++;
					}
					String setTableRulesUpdateTS = "UPDATE " + m_views + " SET unixTime = " + now + " WHERE name ='" + viewName + "'";
					con.executeUpdate(setTableRulesUpdateTS);
				} else {
					throw new RGMAPermanentException("Table or view '" + m_vdbNameUpper + "." + tableName + "' not in schema.");
				}
			}

			/* Update master table timestamp */
			updateMasterTime(con, now);
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
		return true;
	}

	public synchronized void setSchemaRules(List<String> rules) {
		m_vdbRules = rules;
	}

	public synchronized void shutdown() {
	/* Nothing to do */
	}

	public boolean tableExists(String tableName) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			return getTableId(con, tableName) > 0;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	public boolean viewExists(String viewName) throws RGMAPermanentException {
		MySQLConnection con = null;
		try {
			con = new MySQLConnection();
			return getViewId(con, viewName) > 0;
		} catch (SQLException e) {
			throw new RGMAPermanentException(e);
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}

	private void addToResultSetList(MySQLConnection con, List<TupleSet> rsList, String tableName, String predicate) throws SQLException, RGMAPermanentException {
		java.sql.ResultSet jrs = con.executeQuery("SELECT * FROM " + tableName + " " + predicate);
		MetaData metadata = m_metadata.get(tableName);
		TupleSet rs = new TupleSet();
		String row[] = null;
		while (jrs.next()) {
			row = new String[metadata.width];
			for (int i = 1; i <= metadata.width; i++) {
				row[i - 1] = jrs.getString(i);
			}
			rs.addRow(row);
		}
		rsList.add(rs);
	}

	private void appendMetadataColumns(CreateTableStatement cts) throws RGMAPermanentException, RGMAPermanentException {
		for (ColumnDefinition colDef : cts.getColumns()) {
			String colName = colDef.getName().toUpperCase();
			if (colName.startsWith("RGMA")) {
				throw new RGMAPermanentException("Column name '" + colDef.getName() + "' is reserved for RGMA.");
			}
		}
		ColumnDefinition colDef = new ColumnDefinition();
		colDef.setName("RgmaTimestamp");
		colDef.setType(new DataType(Type.TIMESTAMP, 0));
		colDef.setNotNull(true);
		cts.getColumns().add(colDef);

		colDef = new ColumnDefinition();
		colDef.setName("RgmaLRT");
		colDef.setType(new DataType(Type.TIMESTAMP, 0));
		colDef.setNotNull(true);
		cts.getColumns().add(colDef);

		colDef = new ColumnDefinition();
		colDef.setName("RgmaOriginalServer");
		colDef.setType(new DataType(Type.VARCHAR, 255));
		colDef.setNotNull(true);
		cts.getColumns().add(colDef);

		colDef = new ColumnDefinition();
		colDef.setName("RgmaOriginalClient");
		colDef.setType(new DataType(Type.VARCHAR, 255));
		colDef.setNotNull(true);
		cts.getColumns().add(colDef);
	}

	private void buildMetadata(MySQLConnection con, String tableName) throws SQLException, RGMAPermanentException {
		java.sql.ResultSet rs = con.executeQuery("SELECT * FROM " + tableName);
		java.sql.ResultSetMetaData rsmd = rs.getMetaData();
		List<String> colNames = new ArrayList<String>();
		List<DataType> colTypes = new ArrayList<DataType>();
		MetaData md = new MetaData();
		md.m_string = new ArrayList<Boolean>();
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			colNames.add(rsmd.getColumnName(i));
			if (rsmd.getColumnType(i) == java.sql.Types.CHAR) {
				colTypes.add(new DataType(Type.CHAR, rsmd.getPrecision(i)));
				md.m_string.add(true);
			} else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
				colTypes.add(I32);
				md.m_string.add(false);
			} else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
				colTypes.add(I64);
				md.m_string.add(false);
			} else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
				colTypes.add(new DataType(Type.VARCHAR, rsmd.getPrecision(i)));
				md.m_string.add(true);
			} else if (rsmd.getColumnType(i) == -1) {
				colTypes.add(new DataType(Type.VARCHAR, 1024));
				md.m_string.add(true);
			} else {
				throw new RGMAPermanentException(rsmd.getColumnName(i) + " is of type " + rsmd.getColumnType(i) + ". This type is not known");
			}
		}
		md.width = colNames.size();
		m_metadata.put(tableName, md);
	}

	private void checkAuthz(MySQLConnection con, String tableName, UserContextInterface requestContext, char action) throws RGMAPermanentException,
			SQLException {

		if (requestContext == null) { /* Used by internal calls */
			return;
		}

		int tableId = getTableId(con, tableName);
		List<String> authzRules = new ArrayList<String>(m_vdbRules);
		if (tableId != 0) {
			/* Note that only looking for null predicates */
			java.sql.ResultSet rs = con.executeQuery("SELECT credentials, isReadable, isWritable FROM " + m_tableRules
					+ " WHERE predicate IS NULL AND tableID = " + tableId + " ORDER BY ID");
			while (rs.next()) {
				StringBuilder arule = new StringBuilder();
				arule.append(rs.getString("credentials")).append(':');
				if (rs.getString("isReadable").equals("true")) {
					arule.append('R');
				}
				if (rs.getString("isWritable").equals("true")) {
					arule.append('W');
				}
				authzRules.add(arule.toString());
			}
		}

		Expression pred = Authz.constructAuthPredicate(null, requestContext.getDN(), requestContext.getFQANs(), authzRules, Authz.RuleType.SCHEMA, action);

		SQLExpEvaluator evaluator = new SQLExpEvaluator();
		try {
			if (!evaluator.eval(new Tuple(), pred)) {
				throw new RGMAPermanentException("You do not have '" + action + "' permission for this table definition");
			}
		} catch (NullFound e) {
			throw new RGMAPermanentException(e);
		} catch (UnknownAttribute e) {
			throw new RGMAPermanentException("Unknown attribute noted when checking authz rules " + e.getMessage());
		}
	}

	private void checkUpdateTimestampValid(long now) throws RGMAPermanentException {
		if (now < m_masterTime) {
			throw new RGMAPermanentException("It is " + new Date(now) + " but the master time stamp is " + new Date(m_masterTime));
		}
	}

	/**
	 * Create schema database tables. In general the types used are the normal R-GMA types except that TEXT is used for
	 * long strings.
	 * 
	 * @throws SQLException
	 * @throws RGMAPermanentException
	 * @throws RDatabaseException
	 */
	private void createSchemaDatabaseTables(MySQLConnection con, URL masterURL) throws RGMAPermanentException, SQLException {
		StringBuilder query = new StringBuilder();
		query.append("CREATE TABLE ").append(m_tables).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("name VARCHAR(128) NOT NULL").append(',');
		query.append("numOfUserColumns INT NOT NULL").append(',');
		query.append("isSkeleton CHAR(5) NOT NULL").append(',');
		query.append("DN TEXT NOT NULL").append(',');
		query.append("unixTime BIGINT NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		query = new StringBuilder();
		query.append("CREATE TABLE ").append(m_columns).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("name VARCHAR(128) NOT NULL").append(',');
		query.append("tableID INT NOT NULL").append(',');
		query.append("SQLType VARCHAR(20) NOT NULL").append(',');
		query.append("size INT NOT NULL").append(',');
		query.append("isNotNull CHAR(5) NOT NULL").append(',');
		query.append("isPrimaryKey CHAR(5) NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		query = new StringBuilder();
		query.append("CREATE TABLE ").append(m_views).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("name VARCHAR(128) NOT NULL").append(',');
		query.append("tableID INT NOT NULL").append(',');
		query.append("isSkeleton CHAR(5) NOT NULL").append(',');
		query.append("unixTime BIGINT NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		query = new StringBuilder();
		query.append("CREATE TABLE  ").append(m_viewToColumns).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("viewID INT NOT NULL").append(',');
		query.append("columnID INT NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		query = new StringBuilder();
		query.append("CREATE TABLE  ").append(m_indices).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("name VARCHAR(128) NOT NULL").append(',');
		query.append("tableID INT NOT NULL").append(',');
		query.append("isSkeleton CHAR(5) NOT NULL").append(',');
		query.append("unixTime BIGINT NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		query = new StringBuilder();
		query.append("CREATE TABLE  ").append(m_indexToColumns).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("indexID INT NOT NULL").append(',');
		query.append("columnID INT NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		query = new StringBuilder();
		query.append("CREATE TABLE ").append(m_tableRules).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("tableID INT NOT NULL").append(',');
		query.append("predicate TEXT").append(',');
		query.append("credentials TEXT NOT NULL").append(',');
		query.append("isReadable CHAR(5) NOT NULL").append(',');
		query.append("isWritable CHAR(5) NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		query = new StringBuilder();
		query.append("CREATE TABLE ").append(m_viewRules).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("viewID INT NOT NULL").append(',');
		query.append("predicate TEXT NOT NULL").append(',');
		query.append("credentials TEXT NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		query = new StringBuilder();
		query.append("CREATE TABLE  ").append(m_masterTimeTableName).append('(');
		query.append("ID INT NOT NULL").append(',');
		query.append("unixTime BIGINT NOT NULL").append(',');
		query.append("signature INT NOT NULL").append(',');
		query.append("PRIMARY KEY(ID)").append(')');
		con.executeUpdate(query.toString());

		con.executeUpdate("INSERT INTO " + m_masterTimeTableName + " VALUES (1, 0, 0)");
	}

	private void dropIndexBits(MySQLConnection con, int indexID) throws RGMAPermanentException, SQLException {
		con.executeUpdate("DELETE FROM " + m_indexToColumns + " WHERE indexID = " + indexID);
	}

	private void dropTableBits(MySQLConnection con, int tableID) throws RGMAPermanentException, SQLException {
		con.executeUpdate("DELETE FROM " + m_columns + " WHERE tableID=" + tableID);
		con.executeUpdate("DELETE FROM " + m_tableRules + " WHERE tableID=" + tableID);
	}

	private void dropViewBits(MySQLConnection con, int viewId) throws RGMAPermanentException, SQLException {
		con.executeUpdate("DELETE FROM " + m_viewToColumns + " WHERE viewID = " + viewId);
		con.executeUpdate("DELETE FROM " + m_viewRules + " WHERE viewID = " + viewId);
	}

	private int getCount(MySQLConnection con, String tableName) throws RGMAPermanentException, SQLException {
		java.sql.ResultSet rs = con.executeQuery("SELECT COUNT(*) FROM " + tableName);
		rs.first();
		return rs.getInt(1);
	}

	private String getIndexDesc(MySQLConnection con, String indexName, String tableName) throws RGMAPermanentException, SQLException {
		String getColumns = "SELECT c.name FROM  " + m_columns + " c," + m_indexToColumns + " ic," + m_indices + " i," + m_tables + " t WHERE t.name ='"
				+ tableName + "' AND i.name = '" + indexName + "' AND c.tableID = t.ID AND ic.columnID = c.ID AND ic.IndexID = i.ID";
		java.sql.ResultSet rs = con.executeQuery(getColumns);
		StringBuffer desc = new StringBuffer("CREATE INDEX " + indexName + " ON ");
		desc.append(tableName + "(");
		boolean first = true;
		while (rs.next()) {
			if (first) {
				first = false;
			} else {
				desc.append(',');
			}
			desc.append(rs.getString("name"));
		}
		desc.append(')');
		return desc.toString();
	}

	private long getMasterTime(MySQLConnection con) throws SQLException, RGMAPermanentException {
		java.sql.ResultSet rs = con.executeQuery("SELECT unixTime FROM " + m_masterTimeTableName);
		rs.first();
		return rs.getLong("unixTime");
	}

	/**
	 * Returns the first free ID number to insert into the table
	 * 
	 * @param con
	 * @param tableName
	 * @return the number
	 * @throws DatabaseException
	 * @throws SQLException
	 * @throws RGMAPermanentException
	 */
	private int getNext(MySQLConnection con, String tableName) throws SQLException, RGMAPermanentException {
		java.sql.ResultSet rs = con.executeQuery("SELECT MAX(ID) FROM " + tableName);
		rs.first();
		return rs.getInt("MAX(ID)") + 1;
	}

	private int getSignature(MySQLConnection con) throws RGMAPermanentException, SQLException {
		int n = getCount(con, m_tables);
		n = n * 19 + getCount(con, m_columns);
		n = n * 17 + getCount(con, m_tableRules);
		n = n * 13 + getCount(con, m_views);
		n = n * 11 + getCount(con, m_viewToColumns);
		n = n * 7 + getCount(con, m_viewRules);
		n = n * 5 + getCount(con, m_indices);
		n = n * 3 + getCount(con, m_indexToColumns);
		return n;
	}

	/**
	 * Returns a CREATE TABLE statement for the given table. Always of the form: CREATE TABLE tableName (column1 type1
	 * NOT NULL, column2 type2 NOT NULL, PRIMARY KEY (column1, column2)) NB: At the moment, the output from this method
	 * is compared with the parsed table description provided by the user to assess whether two tables have the same
	 * definition.
	 * 
	 * @param tableName
	 *            DOCUMENT ME!
	 * @return DOCUMENT ME!
	 * @throws RGMAPermanentException
	 *             DOCUMENT ME!
	 * @throws DatabaseException
	 * @throws SQLException
	 */
	private String getTableDesc(MySQLConnection con, String tableName) throws RGMAPermanentException, SQLException {
		String getNumOfUserColums = "SELECT numOfUserColumns, ID FROM " + m_tables + " WHERE name = '" + tableName + "'";
		java.sql.ResultSet rs = con.executeQuery(getNumOfUserColums);
		rs.first();
		int userColumnsNum = rs.getInt("numOfUserColumns");
		int tableId = rs.getInt("ID");

		String getColumns = "SELECT name, SQLType, size, isNotNull, isPrimaryKey FROM " + m_columns + " WHERE tableID = " + tableId + " ORDER BY ID LIMIT "
				+ userColumnsNum;
		StringBuffer desc;
		List<String> key = new ArrayList<String>();
		boolean first = true;
		rs = con.executeQuery(getColumns);
		desc = new StringBuffer("CREATE TABLE ");
		desc.append(tableName + " (");
		while (rs.next()) {
			if (first) {
				first = false;
			} else {
				desc.append(',');
			}
			desc.append(rs.getString("name") + " " + rs.getString("SQLType"));
			if (rs.getInt("size") > 0) {
				desc.append("(" + rs.getInt("size") + ")");
			}
			String colName = rs.getString("name");
			if (rs.getString("isPrimaryKey").equalsIgnoreCase("true")) {
				key.add(colName);
				desc.append(" NOT NULL");
			}
		}
		first = true;
		desc.append(", PRIMARY KEY (");
		for (String keyCol : key) {
			if (first) {
				first = false;
			} else {
				desc.append(',');
			}
			desc.append(keyCol);
		}
		desc.append(')');

		desc.append(')');
		return desc.toString();
	}

	/**
	 * returns the tableId that corresponds to a tableName or 0 if not found
	 * 
	 * @throws DatabaseException
	 * @throws SQLException
	 * @throws RGMAPermanentException
	 */
	private int getTableId(MySQLConnection con, String tableName) throws SQLException, RGMAPermanentException {
		String query = "SELECT ID FROM " + m_tables + " WHERE isSkeleton = 'false' AND name = '" + tableName + "'";
		java.sql.ResultSet tableIdResult = con.executeQuery(query);
		if (!tableIdResult.next()) {
			return 0;
		}
		return tableIdResult.getInt("ID");
	}

	private String getViewDesc(MySQLConnection con, String viewName) throws RGMAPermanentException, SQLException {
		String getTableName = "SELECT t.name, v.ID FROM " + m_tables + " t," + m_views + " v WHERE t.ID = tableID and v.name ='" + viewName + "'";
		java.sql.ResultSet rs = con.executeQuery(getTableName);
		rs.first();
		String tableName = rs.getString("t.name");
		int viewID = rs.getInt("v.ID");

		String getColumns = "SELECT name FROM " + m_columns + " c," + m_viewToColumns + " vc WHERE viewId = '" + viewID
				+ "' and columnId = c.ID ORDER BY vc.ID";
		rs = con.executeQuery(getColumns);

		StringBuffer desc = new StringBuffer("CREATE VIEW ").append(viewName).append(" AS SELECT ");
		boolean first = true;
		while (rs.next()) {
			if (first) {
				first = false;
			} else {
				desc.append(',');
			}
			desc.append(rs.getString("name"));
		}
		desc.append(" FROM ").append(tableName);
		return desc.toString();
	}

	/**
	 * returns the tableId that corresponds to a tableName or 0 if not found
	 * 
	 * @throws DatabaseException
	 * @throws SQLException
	 * @throws RGMAPermanentException
	 */
	private int getViewId(MySQLConnection con, String viewName) throws SQLException, RGMAPermanentException {
		String query = "SELECT ID FROM " + m_views + " WHERE isSkeleton = 'false' AND name = '" + viewName + "'";
		java.sql.ResultSet tableIdResult = con.executeQuery(query);
		if (!tableIdResult.next()) {
			return 0;
		}
		return tableIdResult.getInt("ID");
	}

	private boolean isRulesSetMatched(String tableOrViewName, List<String> otherAuthzRules) throws RGMAPermanentException, RGMAPermanentException {
		List<String> authzRulesFromSchema = getAuthorizationRules(tableOrViewName, null);
		if (authzRulesFromSchema.size() != otherAuthzRules.size()) {
			return false;
		}
		return authzRulesFromSchema.containsAll(otherAuthzRules);
	}

	private void putSchemaUpdates(MySQLConnection con, List<TupleSet> records) throws RGMAPermanentException, NumberFormatException, SQLException {
		/* Delete any records hanging onto replaced records first */
		String lastTableName = null;
		int j = 0;
		for (TupleSet r : records) {
			String tableName = m_fullNames[j++];
			List<String[]> data = r.getData();
			if (tableName.equals(m_tables)) {
				for (String[] row : data) {
					dropTableBits(con, Integer.parseInt(row[0]));
				}
			} else if (tableName.equals(m_views)) {
				for (String[] row : data) {
					dropViewBits(con, Integer.parseInt(row[0]));
				}
			} else if (tableName.equals(m_indices)) {
				for (String[] row : data) {
					dropIndexBits(con, Integer.parseInt(row[0]));
				}
			}
			lastTableName = tableName;
		}

		if (lastTableName != null && !lastTableName.equals(m_masterTimeTableName)) {
			throw new RGMAPermanentException("MasterTime table must be last in a replication message");
		}

		/* Then replace records */
		j = 0;
		for (TupleSet r : records) {
			String tableName = m_fullNames[j++];
			StringBuffer replace = new StringBuffer("REPLACE INTO ");
			replace.append(tableName).append(" VALUES ");
			List<String[]> data = r.getData();
			MetaData metadata = m_metadata.get(tableName);
			boolean first = true;
			for (String[] row : data) {
				if (first) {
					first = false;
				} else {
					replace.append(',');
				}
				replace.append('(');
				boolean innerfirst = true;
				for (int i = 0; i < metadata.width; i++) {
					if (innerfirst) {
						innerfirst = false;
					} else {
						replace.append(',');
					}
					if (metadata.m_string.get(i)) {
						if (row[i] == null) {
							replace.append("null");
						} else {
							replace.append('\'').append(row[i].replaceAll("'", "\\\\'")).append("'");
						}
					} else {
						replace.append(row[i]);
					}
				}
				replace.append(')');

			}
			if (!first) {
				con.executeUpdate(replace.toString());
			}
		}
		if (records.size() != 0) {
			refreshCache(con);
			java.sql.ResultSet rs = con.executeQuery("SELECT signature FROM " + m_masterTimeTableName);
			rs.first();
			int sigInMessage = rs.getInt("signature");
			int signature = getSignature(con);
			if (sigInMessage != signature) {
				throw new RGMAPermanentException("Computed signature " + signature + " does not match received one " + sigInMessage);
			}
		}
	}

	private void refreshCache(MySQLConnection con) throws SQLException, RGMAPermanentException {
		m_tablesNext = getNext(con, m_tables);
		m_tableRulesNext = getNext(con, m_tableRules);
		m_columnsNext = getNext(con, m_columns);
		m_viewsNext = getNext(con, m_views);
		m_viewToColumnsNext = getNext(con, m_viewToColumns);
		m_viewRulesNext = getNext(con, m_viewRules);
		m_indicesNext = getNext(con, m_indices);
		m_indexToColumnsNext = getNext(con, m_indexToColumns);
		m_masterTime = getMasterTime(con);
	}

	private void updateMasterTime(MySQLConnection con, long updateTime) throws RGMAPermanentException, SQLException {
		int signature = getSignature(con);
		con.executeUpdate("UPDATE " + m_masterTimeTableName + " SET unixTime = " + updateTime + ", signature = " + signature);
		m_masterTime = updateTime;
	}

}