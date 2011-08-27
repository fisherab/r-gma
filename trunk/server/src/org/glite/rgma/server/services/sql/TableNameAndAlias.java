/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

/**
 * A table name with an optional alias.
 */
public class TableNameAndAlias extends TableName {

	private String m_alias;

	public TableNameAndAlias(String vdbName, String tableName) {
		super(vdbName, tableName);
	}

	public TableNameAndAlias(TableNameAndAlias other) {
		super(other);
		m_alias = other.m_alias;
	}

	public TableNameAndAlias(TableName tableName) {
		super(tableName);
	}

	public TableNameAndAlias(String value) {
		super(value);
	}

	public void setAlias(String alias) {
		m_alias = alias;
	}
	
	public String getAlias() {
		return m_alias;
	}

	public String toString() {
		if (m_alias == null) {
			return super.toString();
		} else {
			return super.toString() + " AS " + m_alias;
		}
	}
}
