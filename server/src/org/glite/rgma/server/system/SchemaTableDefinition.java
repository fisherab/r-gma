/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.system;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.glite.rgma.server.services.sql.CreateTableStatement;
import org.glite.rgma.server.services.sql.parser.ParseException;

/**
 * Contains the column definitions for a table.
 */
public class SchemaTableDefinition {
	/** Table name */
	private final String m_name;

	/** Column definitions */
	private final List<SchemaColumnDefinition> m_columns;

	/** For a view, the name of the underlying table. */
	private final String m_viewFor;

	/** A dynamically created map for looking up columns */
	private Map<String, SchemaColumnDefinition> m_fromUpper;

	/**
	 * Creates a new table definition object
	 * 
	 * @param tableName
	 *            The table name.
	 * @param columns
	 *            List of columns that define the table.
	 */
	public SchemaTableDefinition(String tableName, List<SchemaColumnDefinition> columns) {
		m_name = tableName;
		m_viewFor = null;
		m_columns = columns;
	}

	/**
	 * Creates a new table definition object for a view
	 * 
	 * @param viewName
	 *            The view name.
	 * @param viewFor
	 *            The name of the table that this is a view of.
	 * @param columns
	 *            List of columns that define the table.
	 */
	public SchemaTableDefinition(String viewName, String viewFor, List<SchemaColumnDefinition> columns) {
		m_name = viewName;
		m_viewFor = viewFor;
		m_columns = columns;
	}

	public synchronized SchemaColumnDefinition getColumn(String name) {
		if (m_fromUpper == null) {
			m_fromUpper = new HashMap<String, SchemaColumnDefinition>();
			for (SchemaColumnDefinition col : m_columns) {
				m_fromUpper.put(col.getName().toUpperCase(), col);
			}
		}
		return m_fromUpper.get(name.toUpperCase());
	}

	/**
	 * @return Returns the table columns.
	 */
	public List<SchemaColumnDefinition> getColumns() {
		return m_columns;
	}

	/**
	 * @return The table definition as a string in the form of an SQL CREATE TABLE statement.
	 */
	public CreateTableStatement getCreateTableStmt() {
		StringBuffer str = new StringBuffer();
		StringBuffer key = new StringBuffer();

		for (int i = 0; i < m_columns.size(); i++) {
			SchemaColumnDefinition colDef = m_columns.get(i);

			if (str.length() > 0) {
				str.append(", ");
			}

			str.append(colDef.getName());
			str.append(" " + colDef.getType());

			if (colDef.isNotNull()) {
				str.append(" NOT NULL");
			}

			if (colDef.isPrimaryKey()) {
				if (key.length() > 0) {
					key.append(", ");
				}

				key.append(colDef.getName());
			}
		}

		str.insert(0, "CREATE TABLE " + m_name + " (");

		if (key.length() > 0) {
			key.insert(0, ", PRIMARY KEY (");
			key.append(")");
			str.append(key);
		}

		str.append(")");
		CreateTableStatement cts = null;
		try {
			cts = CreateTableStatement.parse(str.toString());
		} catch (ParseException pe) {}
		return cts;
	}

	/**
	 * @return Returns the tableName.
	 */
	public String getTableName() {
		return m_name;
	}

	/**
	 * @return Returns the name of the underlying table for a view, <code>null</code> for a table.
	 */
	public String getViewFor() {
		return m_viewFor;
	}

	/**
	 * Test if this table definition is consistent with another.
	 * 
	 * @return <code>true</code> if the two tables have exactly the same column definitions, <code>false</code>
	 *         otherwise. It is not necessary for the two tables to have the same name.
	 */
	public boolean isConsistentWith(SchemaTableDefinition def) {
		if (m_columns.size() != def.m_columns.size()) {
			return false;
		}

		for (int i = 0; i < m_columns.size(); i++) {
			if (!m_columns.get(i).equals(def.m_columns.get(i))) {
				return false;
			}
		}

		return true;
	}

	/**
	 * @return <code>true</code> for a view, <code>false</code> otherwise.
	 */
	public boolean isView() {
		return m_viewFor != null;
	}

	/**
	 * @see Object#toString
	 */
	@Override
	public String toString() {
		return m_name + (m_viewFor != null ? " is view on " + m_viewFor : "") + ", " + m_columns;
	}

}