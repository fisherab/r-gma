/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.ondemand;

import java.util.Hashtable;
import java.util.Map;

import org.glite.rgma.server.services.sql.DataType;
import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * Column and table details for a ResultSet - only used by the ODP
 */
class OnDemandMetaData {
	/** Mapping from column name (in upper case) to index in arrays. */
	private Map<String, Integer> m_columnsByName;

	/** Array of column names. */
	private String[] m_columnNames;

	/** Number of columns. */
	private int m_columnCount;

	/** Array of table names. */
	private String[] m_tableNames;

	/** Array of column data types. */
	private DataType[] m_columnTypes;

	public OnDemandMetaData(String[] columnNames, String[] tableNames, DataType[] columnTypes) throws RGMAPermanentException {
		m_columnCount = columnNames.length;
		if (m_columnCount != columnTypes.length) {
			throw new RGMAPermanentException("Column names and Types must be same length for table " + tableNames[0]);
		}
		if (m_columnCount != tableNames.length) {
			throw new RGMAPermanentException("Column names and Table names must be same length");
		}
		m_columnsByName = new Hashtable<String, Integer>(columnNames.length);
		m_columnNames = columnNames;
		m_columnTypes = columnTypes;
		m_tableNames = tableNames;

		for (int i = 0; i < columnNames.length; i++) {
			m_columnsByName.put(columnNames[i].toUpperCase(), i + 1);
		}
	}

	/**
	 * Returns the number of columns in this <code>ResultSet</code> object.
	 * 
	 * @return the number of columns
	 */
	public int getColumnCount() {
		return m_columnCount;
	}

	/**
	 * Get the designated column's name.
	 * 
	 * @param column
	 *            the first column is 1, the second is 2, ...
	 * @return column name
	 * @exception RGMAPermanentException
	 *                if an RGMA access error occurs
	 */
	public String getColumnName(int column) {
		return m_columnNames[column - 1];
	}

	/**
	 * Returns the name of the table.
	 * 
	 * @return A String array with the table names.
	 */
	public String[] getTableNames() {
		return m_tableNames;
	}

	/**
	 * Returns the column number of the given column.
	 * 
	 * @param columnName
	 *            Name of column.
	 * @return -1 if column doesn't exist
	 */
	public int getColumnNumber(String columnName) {
		Integer colNum = m_columnsByName.get(columnName.toUpperCase());
		int columnNumber = colNum == null ? -1 : colNum.intValue();

		return columnNumber;
	}

	/**
	 * Retrieves the designated column's SQL type.
	 * 
	 * @param column
	 *            the first column is 1, the second is 2, ...
	 * @return SQL type from DataType.
	 */
	public DataType getColumnType(int column) {
		return m_columnTypes[column - 1];
	}

}
