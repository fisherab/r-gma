/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

import java.util.ArrayList;
import java.util.List;

/**
 * A set of tuples.
 */
public class TupleSet {

	/** List of row data info (String[]). */
	private final List<String[]> m_data;

	/** Warning about quality of this result. */
	private String m_warning;

	/**
	 * Indicates that this ResultSet contains all the remaining data for the query.
	 */
	private boolean m_endOfResults;

	public TupleSet() {
		m_data = new ArrayList<String[]>();
	}

	public TupleSet(int capacity) {
		m_data = new ArrayList<String[]>(capacity);
	}

	public List<String[]> getData() {
		return m_data;
	}

	public String getWarning() {
		return m_warning;
	}

	public void setWarning(String warning) {
		m_warning = warning;
	}

	public void addRow(String[] row) {
		m_data.add(row);
	}

	public void addRows(List<String[]> rows) {
		m_data.addAll(rows);
	}

	public void setEndOfResults(boolean endOfResults) {
		m_endOfResults = endOfResults;
	}

	public boolean isEndOfResults() {
		return m_endOfResults;
	}

	public int size() {
		return m_data.size();
	}
}
