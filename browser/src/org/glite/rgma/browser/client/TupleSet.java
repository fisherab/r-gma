/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.browser.client;

import java.util.List;

/**
 * Apart from the package this is a copy from the Java API
 */
public class TupleSet {

	/** Warning about quality of this result. */
	private String m_warning;

	/** Vector of row data info (String[]). */
	private List<String[]> m_data;

	/** Indicates that this tuple set contains all the remaining data for the query. */
	private boolean m_endOfData;

	TupleSet(List<String []> data, boolean endOfData, String warning) {
		m_data = data;
		m_endOfData = endOfData;
		m_warning = warning;
	}

	/**
	 * Gets the list of tuples. It is possible to modify the list and even include tuples from different tables.
	 * 
	 * @return list of tuples. The list may be empty.
	 */
	public List<String[]> getData() {
		return m_data;
	}

	/**
	 * Gets the warning string associated with this tuple set.
	 * 
	 * @return the warning string associated with this tuple set. It will be an empty string if no warning has been set.
	 */
	public String getWarning() {
		return m_warning;
	}

	void appendWarning(String warning) {
		if (m_warning.length() == 0) {
			m_warning = warning;
		} else {
			m_warning = m_warning + " " + warning;
		}
	}

	/**
	 * Reports whether this tuple set is the last one to be returned by a consumer.
	 * 
	 * @return <code>true</code> if there are no more tuple sets that need to be popped for this query
	 */
	public boolean isEndOfResults() {
		return m_endOfData;
	}

}
