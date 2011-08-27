/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.util.ArrayList;
import java.util.List;

/**
 * A schema allows tables to be created and their definitions manipulated for a specific VDB.
 */
public class Schema extends VDB {

	/**
	 * Constructs a schema object for the specified VDB.
	 * 
	 * @param vdbName
	 *            name of VDB
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Schema(String vdbName) throws RGMAPermanentException, RGMATemporaryException {
		m_servletName = "SchemaServlet";
		m_vdbName = vdbName;
	}

	/**
	 * Creates an index on a table.
	 * 
	 * @param createIndexStatement
	 *            SQL CREATE INDEX statement
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void createIndex(String createIndexStatement) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("createIndexStatement", createIndexStatement);
		checkOK(connection.sendNonResourceCommand("createIndex"));
	}

	/**
	 * Creates a table in the schema.
	 * 
	 * @param createTableStatement
	 *            SQL CREATE TABLE statement
	 * @param authzRules
	 *            list of authorization rules
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void createTable(String createTableStatement, List<String> authzRules) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("createTableStatement", createTableStatement);

		for (String rule : authzRules) {
			connection.addParameter("tableAuthz", rule);
		}
		checkOK(connection.sendNonResourceCommand("createTable"));
	}

	/**
	 * Creates a view on a table.
	 * 
	 * @param createViewStatement
	 *            SQL CREATE VIEW statement
	 * @param authzRules
	 *            list of authorization rules
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void createView(String createViewStatement, List<String> authzRules) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("createViewStatement", createViewStatement);

		for (String rule : authzRules) {
			connection.addParameter("viewAuthz", rule);
		}
		checkOK(connection.sendNonResourceCommand("createView"));
	}

	/**
	 * Drops an index from a table.
	 * 
	 * @param tableName
	 *            name of table with index to drop
	 * @param indexName
	 *            name of index to drop
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void dropIndex(String tableName, String indexName) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("tableName", tableName);
		connection.addParameter("canForward", true);
		connection.addParameter("indexName", indexName);
		checkOK(connection.sendNonResourceCommand("dropIndex"));
	}

	/**
	 * Drops a table from the schema.
	 * 
	 * @param name
	 *            table to drop
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void dropTable(String name) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("tableName", name);
		checkOK(connection.sendNonResourceCommand("dropTable"));
	}

	/**
	 * Drops a view from the schema.
	 * 
	 * @param name
	 *            view to drop
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void dropView(String name) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("viewName", name);
		checkOK(connection.sendNonResourceCommand("dropView"));
	}

	/**
	 * Gets a list of all tables and views in the schema.
	 * 
	 * @return list of table names
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public List<String> getAllTables() throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		List<String> sl = new ArrayList<String>();
		for (Tuple t : connection.sendNonResourceCommand("getAllTables").getData()) {
			sl.add(t.getString(0));
		}
		return sl;
	}

	/**
	 * Gets the authorization rules for a table or view.
	 * 
	 * @param name
	 *            name of table or view
	 * @return list of authorization rules
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 * @see #setAuthorizationRules
	 */
	public List<String> getAuthorizationRules(String name) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("tableName", name);
		List<String> tableAuthz = new ArrayList<String>();
		for (Tuple t : connection.sendNonResourceCommand("getAuthorizationRules").getData()) {
			String columnName = t.getString(0);
			tableAuthz.add(columnName);
		}
		return tableAuthz;
	}

	/**
	 * Gets the definition for the named table or view.
	 * 
	 * @param name
	 *            name of table or view
	 * @return definition
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public TableDefinition getTableDefinition(String name) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("tableName", name);
		List<Tuple> ts = connection.sendNonResourceCommand("getTableDefinition").getData();
		List<ColumnDefinition> columnList = new ArrayList<ColumnDefinition>();

		String tableName = ts.get(0).getString(0);
		String viewFor = ts.get(0).getString(6);

		for (Tuple t : ts) {
			String columnName = t.getString(1);
			RGMAType type;
			try {
				type = RGMAType.valueOf(t.getString(2));
			} catch (IllegalArgumentException e) {
				throw new RGMAPermanentException("Internal problem: " + t.getString(2) + " is not a valid name for a SQL type");
			}
			int size = t.getInt(3);
			boolean isNotNull = t.getBoolean(4);
			boolean isPrimaryKey = t.getBoolean(5);
			ColumnDefinition columnDef = new ColumnDefinition(columnName, type, size, isNotNull, isPrimaryKey);
			columnList.add(columnDef);
		}
		return new TableDefinition(tableName, viewFor, columnList);
	}

	/**
	 * Gets the list of indexes for a table.
	 * 
	 * @param name
	 *            name of table
	 * @return list of index objects
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public List<Index> getTableIndexes(String name) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("tableName", name);

		List<Index> indexes = new ArrayList<Index>();
		String currentIndex = null;

		List<String> columnNames = new ArrayList<String>();
		for (Tuple t : connection.sendNonResourceCommand("getTableIndexes").getData()) {
			String indexName = t.getString(0);
			String columnName = t.getString(1);

			if (currentIndex == null) {
				currentIndex = indexName;
				columnNames.add(columnName);
			} else {
				if (currentIndex.equals(indexName)) {
					columnNames.add(columnName);
				} else {
					Index index = new Index(currentIndex, columnNames);
					indexes.add(index);
					columnNames = new ArrayList<String>();
					columnNames.add(columnName);
					currentIndex = indexName;
				}
			}
		}

		if (currentIndex != null) {
			Index index = new Index(currentIndex, columnNames);
			indexes.add(index);
		}
		return indexes;
	}

	/**
	 * Sets the authorization rules for the given table or view.
	 * 
	 * @param name
	 *            name of table or view
	 * @param authzRules
	 *            list of authorization rules
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 * @see #getAuthorizationRules
	 */
	public void setAuthorizationRules(String name, List<String> authzRules) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("tableName", name);
		for (String rule : authzRules) {
			connection.addParameter("tableAuthz", rule);
		}
		checkOK(connection.sendNonResourceCommand("setAuthorizationRules"));
	}

	private void checkOK(TupleSet ts) throws RGMAPermanentException {
		String status = ts.getData().get(0).getString(0);
		if (status.equals("true") || status.equals("false")) {
			return;
		}
		throw new RGMAPermanentException("Failed to return status of True or False");
	}
}
