/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.util.StringTokenizer;

/**
 * A table name.
 */
public class TableName {

	private String m_vdbName, m_tableName;

	public TableName(String vdbName, String tableName) {
		m_vdbName = vdbName.toUpperCase();
		m_tableName = tableName.toUpperCase();
	}

	public TableName(TableName other) {
		m_vdbName = other.m_vdbName;
		m_tableName = other.m_tableName;
	}
	
	public TableName(String value) {
		StringTokenizer st = new StringTokenizer(value.toUpperCase(), ".");
		if (st.countTokens() == 2) {
			m_vdbName = st.nextToken();
		}
		if (st.countTokens() == 1) {
			m_tableName = st.nextToken();
		}
	}

	public String getVdbName() {
		return m_vdbName;
	}

	public String getTableName() {
		return m_tableName;
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
		return getVdbTableName();
	}
}
