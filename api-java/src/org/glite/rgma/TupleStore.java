/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

/**
 * Details of a named tuple store.
 */
public class TupleStore {

	private String m_logicalName;
	private boolean m_isHistory;
	private boolean m_isLatest;

	protected TupleStore(String logicalName, boolean isHistory, boolean isLatest) {
		m_logicalName = logicalName;
		m_isHistory = isHistory;
		m_isLatest = isLatest;
	}

	/**
	 * Returns the logical name for this tuple store.
	 * 
	 * @return logical name as a string
	 */
	public String getLogicalName() {
		return m_logicalName;
	}

	/**
	 * Determines if this tuple store supports history queries.
	 * 
	 * @return <code>true</code> if this tuple store supports history queries
	 */
	public boolean isHistory() {
		return m_isHistory;
	}

	/**
	 * Determines if this tuple store supports latest queries.
	 * 
	 * @return <code>true</code> if this tuple store supports latest queries
	 */
	public boolean isLatest() {
		return m_isLatest;
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
		return "TupleStore " + m_logicalName + (m_isHistory ? " isHistory" : "") + (m_isLatest ? " isLatest" : "");
	}
}
