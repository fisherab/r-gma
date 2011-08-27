/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

/**
 * A column name with an optional alias. The column name may be of the form "col" or "table.col" or "vdb.table.col"
 */
public class ColumnNameAndAlias extends ColumnName {

	private String m_alias;

	public ColumnNameAndAlias(String vdbName, String tableName, String columnName) {
		super(vdbName, tableName, columnName);
	}

	public ColumnNameAndAlias(String value) {
		super(value);
	}

	public void setAlias(String alias) {
		m_alias = alias;
	}

	public String getAlias() {
		return m_alias;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(super.toString());
		if (m_alias != null) {
			sb.append(" AS ").append(m_alias);
		}
		return sb.toString();
	}
}
