/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.mediator;

/**
 * Details of a table declared by a producer.
 */
public class DeclaredTable {

	/**
	 * Table name.
	 */
	protected String m_tableName;

	/**
	 * History retention period in seconds
	 */
	protected long m_hrpSec;

	/**
	 * The producer's predicate for this table.
	 */
	protected String m_predicate;

	/**
	 * Creates a new DeclaredTable object.
	 */
	public DeclaredTable(String tableName, long hrpSec, String predicate) {
		m_tableName = tableName;
		m_hrpSec = hrpSec;
		m_predicate = predicate;
	}

	/**
	 * @see Object#toString
	 */
	public String toString() {
		StringBuffer str = new StringBuffer();
		str.append(m_tableName);
		str.append("/hrp=").append(m_hrpSec);
		if (m_predicate.length() != 0) {
			str.append("/pred=").append(m_predicate);
		}
		return str.toString();
	}

	/**
	 * Compares the specified object with this producer.
	 * 
	 * If obj is a DeclaredTable object, test if it represents the same table (i.e. has the same
	 * table name).
	 * 
	 * @param obj
	 *            Object to compare.
	 * @return <code>true</code> if the object is equal to this producer.
	 */
	public boolean equals(Object obj) {
		if (!(obj instanceof DeclaredTable)) {
			return false;
		}

		DeclaredTable dt = (DeclaredTable) obj;

		if (!m_tableName.equals(dt.m_tableName)) {
			return false;
		}

		if (!m_predicate.equals(dt.m_predicate)) {
			return false;
		}

		return true;
	}

	public int hashCode() {
		return (m_tableName + m_predicate).hashCode();
	}

	/**
	 * Returns the tableName.
	 */
	public String getTableName() {
		return m_tableName;
	}

	/**
	 * Returns the producer's history retention period.
	 */
	public long getHistoryRetentionPeriod() {
		return m_hrpSec;
	}

	/**
	 * Returns the producer's predicate for this table.
	 */
	public String getPredicate() {
		return m_predicate;
	}
}
