/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.util.List;

/**
 * An index on a table.
 */
public class Index {
	/** Name of index. */
	private String m_indexName;

	/** List of column names (String objects). */
	private List<String> m_columnNames;

	/**
	 * Creates a new index object.
	 * 
	 * @param name
	 *            name of index
	 * @param columnNames
	 *            list of column names in index
	 */
	Index(String name, List<String> columnNames) {
		m_indexName = name;
		m_columnNames = columnNames;
	}

	/**
	 * Returns the list of column names used by this index.
	 * 
	 * @return list of column names
	 */
	public List<String> getColumnNames() {
		return m_columnNames;
	}

	/**
	 * Returns the name of this index.
	 * 
	 * @return the name of this index - this is local to the table
	 */
	public String getIndexName() {
		return m_indexName;
	}

	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 * @see Object#toString()
	 */
	public String toString() {
		return "Index[" + m_indexName + ", columns=" + m_columnNames + "]";
	}
}
