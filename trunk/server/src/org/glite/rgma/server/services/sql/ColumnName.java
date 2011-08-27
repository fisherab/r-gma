/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.util.StringTokenizer;

/**
 * A column name. The column name may be of the form "col" or "table.col" or "vdb.table.col"
 */
public class ColumnName {

	private String m_vdbName, m_tableName, m_columnName;

	public ColumnName(String vdbName, String tableName, String columnName) {
		if (vdbName != null) {
			m_vdbName = vdbName.toUpperCase();
		}
		if (tableName != null) {
			m_tableName = tableName.toUpperCase();
		}
		m_columnName = columnName.toUpperCase();
	}

	public ColumnName(String value) {
		StringTokenizer st = new StringTokenizer(value.toUpperCase(), ".");
		if (st.countTokens() == 3) {
			m_vdbName = st.nextToken();
		}
		if (st.countTokens() == 2) {
			m_tableName = st.nextToken();
		}
		if (st.countTokens() == 1) {
			m_columnName = st.nextToken();
		}
	}

	public String getVdbName() {
		return m_vdbName;
	}

	public String getTableName() {
		return m_tableName;
	}

	public String getColumnName() {
		return m_columnName;
	}

	public String getVdbTableName() {
		StringBuilder sb = new StringBuilder();
		if (m_vdbName != null) {
			sb.append(m_vdbName).append('.');
		}
		if (m_tableName != null) {
			sb.append(m_tableName);
			return sb.toString();
		} else {
			return null;
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (m_vdbName != null) {
			sb.append(m_vdbName).append('.');
		}
		if (m_tableName != null) {
			sb.append(m_tableName).append('.');
		}
		sb.append(m_columnName);
		return sb.toString();
	}
}
