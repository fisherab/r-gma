/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.util.List;

/**
 * Definitions of a table, including table name and column details.
 */
public class TableDefinition {
	/** Table name. */
	private String m_tableName;

	/** Column definitions. */
	private List<ColumnDefinition> m_columns;

	private String m_viewFor;

	TableDefinition(String viewName, String viewFor, List<ColumnDefinition> columns) {
		m_tableName = viewName;
		m_viewFor = viewFor;
		m_columns = columns;
	}

	/**
	 * Returns the column definitions for this table.
	 * 
	 * @return column definitions for this table
	 */
	public List<ColumnDefinition> getColumns() {
		return m_columns;
	}

	/**
	 * Returns the name of this table.
	 * 
	 * @return the name of this table
	 */
	public String getTableName() {
		return m_tableName;
	}

	/**
	 * Returns name of table for which this is a view or null.
	 * 
	 * @return name of table for which this is a view or null
	 */
	public String getViewFor() {
		return m_viewFor;
	}

	/**
	 * Returns true if this is a view rather than a table.
	 * 
	 * @return <code>true</code> if this is a view rather than a table
	 */
	public boolean isView() {
		return m_viewFor != null;
	}

	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 * @see Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer("TableDefinition[\n");
		str.append("table name=" + m_tableName);
		str.append(", columns=" + m_columns);
		str.append("]\n");

		return str.toString();
	}
}
