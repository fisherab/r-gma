/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

import org.glite.rgma.server.services.sql.DataType;

/**
 * Contains the definition of a single column from the Schema.
 */
public class SchemaColumnDefinition {
	/**
	 * Column name.
	 */
	private final String m_name;

	/**
	 * Contains the definition
	 */
	private final DataType m_type;

	/**
	 * True if column may not be NULL.
	 */
	private final boolean m_notNull;

	/**
	 * True if column forms part of the primary key.
	 */
	private final boolean m_primaryKey;

	/**
	 * Constructor.
	 */
	public SchemaColumnDefinition(String columnName, DataType type, boolean isNotNull, boolean primaryKey) {
		m_name = columnName;
		m_type = type;
		m_notNull = isNotNull;
		m_primaryKey = primaryKey;
	}

	/**
	 * @return Returns the column name.
	 */
	public String getName() {
		return m_name;
	}

	/**
	 * @return Returns True if column may not be null.
	 */
	public boolean isNotNull() {
		return m_notNull;
	}

	/**
	 * @return Returns True if column forms part of the primaryKey.
	 */
	public boolean isPrimaryKey() {
		return m_primaryKey;
	}

	/**
	 * @return Returns the SQLType type.
	 */
	public DataType getType() {
		return m_type;
	}

	/**
	 * @see Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj instanceof SchemaColumnDefinition) {
			SchemaColumnDefinition cd = (SchemaColumnDefinition) obj;
			if (m_name.equalsIgnoreCase(cd.m_name) && m_type.equals(cd.m_type) && (m_notNull == cd.m_notNull) && (m_primaryKey == cd.m_primaryKey)) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public int hashCode() {
		int code = m_name.hashCode() + m_type.hashCode();
		if (m_notNull)
			code++;
		if (m_primaryKey)
			code++;
		return code;
	}

	/**
	 * @see Object#toString
	 */
	public String toString() {
		return m_type + " " + m_name + (m_notNull ? " not null" : "")
				+ (m_primaryKey ? " primary key" : "");
	}
}
