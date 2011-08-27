/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.util.HashMap;
import java.util.Map;

/**
 * Definition of a column in an SQL CREATE TABLE statement.
 */
public class ColumnDefinition {
	/**
	 * Mappings giving standard name for types that can have more than one name. Only mapping in R-GMA SQL is between
	 * INT and INTEGER.
	 */
	private static final Map<String, String> TYPE_EQUIVALENCES = new HashMap<String, String>();

	static {
		TYPE_EQUIVALENCES.put("INT", "INTEGER");
	}

	/** Column name. */
	private String m_name;

	/** Column type. */
	private DataType m_type;

	/** Not null flag. */
	private boolean m_notNull;

	/** Primary key flag. */
	private boolean m_primaryKey;

	/**
	 * Create empty ColumnDefinition.
	 */
	public ColumnDefinition() {}

	/**
	 * Create copy of given ColumnDefinition.
	 */
	public ColumnDefinition(ColumnDefinition def) {
		m_name = def.m_name;
		m_type = def.m_type;
		m_notNull = def.m_notNull;
		m_primaryKey = def.m_primaryKey;
	}

	/**
	 * Sets the name.
	 * 
	 * @param name
	 *            The name to set
	 */
	public void setName(String name) {
		m_name = name;
	}

	/**
	 * Returns the name.
	 * 
	 * @return Column name.
	 */
	public String getName() {
		return m_name;
	}

	/**
	 * Sets the NOT NULL flag.
	 * 
	 * @param notNull
	 *            The NOT NULL flag.
	 */
	public void setNotNull(boolean notNull) {
		m_notNull = notNull;
	}

	/**
	 * Returns the NOT NULL flag.
	 * 
	 * @return The NOT NULL flag.
	 */
	public boolean isNotNull() {
		return m_notNull;
	}

	/**
	 * Sets the PRIMARY KEY flag.
	 * 
	 * @param primaryKey
	 *            The PRIMARY KEY flag.
	 */
	public void setPrimaryKey(boolean primaryKey) {
		m_primaryKey = primaryKey;
	}

	/**
	 * Returns the PRIMARY KEY flag.
	 * 
	 * @return The PRIMARY KEY flag.
	 */
	public boolean isPrimaryKey() {
		return m_primaryKey;
	}

	/**
	 * Sets the type.
	 * 
	 * @param type
	 *            The type to set.
	 */
	public void setType(DataType type) {
		m_type = type;
	}

	/**
	 * Returns the type.
	 * 
	 * @return The column type.
	 */
	public DataType getType() {
		return m_type;
	}

	/**
	 * @see Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		if (!(obj instanceof ColumnDefinition)) {
			return false;
		}

		ColumnDefinition cd = (ColumnDefinition) obj;

		boolean equals = (this.getName().equals(cd.getName())) && (getType().equals(cd.getType())) && (this.isNotNull() == cd.isNotNull())
				&& (this.isPrimaryKey() == cd.isPrimaryKey());

		return equals;
	}

	/**
	 * Returns object as a string.
	 * 
	 * @return Object as a String.
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer(m_name);
		buf.append(" " + m_type);

		if (m_notNull || m_primaryKey) {
			buf.append(" NOT NULL");
		}

		return buf.toString();
	}
}
