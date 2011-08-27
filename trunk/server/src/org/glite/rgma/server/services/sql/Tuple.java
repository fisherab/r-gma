/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.util.HashMap;
import java.util.Map;

/**
 * A relational tuple (row in a table). This consists of a set of column names (as String objects) and a set of column
 * values, each of which is a String, Integer/Double or null, representing STRING, NUMERIC and NULL Constant types. This
 * class may contain redundant material.
 */
public class Tuple {

	@SuppressWarnings("serial")
	public class UnknownAttribute extends Exception {

		public UnknownAttribute(String name) {
			super(name);
		}
	}

	/** Mapping from upper cased attribute name to index in columnValues. */
	private Map<String, Object> m_searchTable = new HashMap<String, Object>();

	public void addAttribute(String name, Object value) {
		m_searchTable.put(name.toUpperCase(), value);
	}

	public Object getAttValue(String name) throws UnknownAttribute {
		Object obj = m_searchTable.get(name.toUpperCase());
		if (obj == null && !m_searchTable.containsKey(name)) {
			throw new UnknownAttribute(name);
		}
		return obj;
	}
}