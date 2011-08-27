/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

/**
 * Definition of a column which may be used inside a {@link TableDefinition}.
 */
public class ColumnDefinition {
	/** Column name. */
	private String m_name;

	/** Column width for CHAR and VARCHAR types. */
	private int m_size;

	/** Column type */
	private RGMAType m_type;

	/** NOT NULL flag. */
	private boolean m_notNull;

	/** PRIMARY KEY flag. */
	private boolean m_primaryKey;

	ColumnDefinition(String name, RGMAType type, int size, boolean isNotNull, boolean isPrimaryKey) {
		m_name = name;
		m_type = type;
		m_size = size;
		m_notNull = isNotNull;
		m_primaryKey = isPrimaryKey;
	}

	/**
	 * Returns the name of the column.
	 * 
	 * @return the name of the column
	 */
	public String getName() {
		return m_name;
	}

	/**
	 * Returns the size of the column type.
	 * 
	 * @return the size of the column type (or 0 if none is specified)
	 */
	public int getSize() {
		return m_size;
	}

	/**
	 * Returns the type of the column.
	 * 
	 * @return the type of the column
	 */
	public RGMAType getType() {
		return m_type;
	}

	/**
	 * Returns the NOT NULL flag.
	 * 
	 * @return <code>true</code> if column is "NOT NULL"
	 */
	public boolean isNotNull() {
		return m_notNull;
	}

	/**
	 * Returns the PRIMARY KEY flag.
	 * 
	 * @return <code>true</code> if column is "PRIMARY KEY"
	 */
	public boolean isPrimaryKey() {
		return m_primaryKey;
	}

	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 * 
	 * @see Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder(m_name + " " + m_type.name());
		if (m_size > 0) {
			buf.append("(" + m_size + ")");
		}
		if (m_notNull) {
			buf.append(" NOT NULL");
		}
		if (m_primaryKey) {
			buf.append(" PRIMARY KEY");
		}
		return buf.toString();
	}
}
