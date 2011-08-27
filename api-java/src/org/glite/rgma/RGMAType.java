/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.util.HashMap;
import java.util.Map;

/**
 * Constants for SQL column types.
 */
public enum RGMAType {
	/** SQL INTEGER. */
	INTEGER,

	/** SQL REAL. */
	REAL,

	/** SQL DOUBLE. */
	DOUBLE,

	/** SQL DATE. */
	DATE,

	/** SQL TIME. */
	TIME, 
	
	/** SQL TIMESTAMP. */
	TIMESTAMP, 
	
	/** SQL CHAR. */
	CHAR, 
	
	/** SQL VARCHAR. */
	VARCHAR;

	/** Mapping from type numbers to types. */
	private static final Map<Integer, RGMAType> TYPE_ENUMS = new HashMap<Integer, RGMAType>();

	static {
		TYPE_ENUMS.put(4, INTEGER);
		TYPE_ENUMS.put(7, REAL);
		TYPE_ENUMS.put(8, DOUBLE);
		TYPE_ENUMS.put(91, DATE);
		TYPE_ENUMS.put(92, TIME);
		TYPE_ENUMS.put(93, TIMESTAMP);
		TYPE_ENUMS.put(1, CHAR);
		TYPE_ENUMS.put(12, VARCHAR);
	}

	/**
	 * Gets the type associated with a type number.
	 * 
	 * @param type
	 *            Type number.
	 * 
	 * @return Name associated with <code>type</code>.
	 */
	static RGMAType getType(int type) {
		return TYPE_ENUMS.get(type);
	}

}
