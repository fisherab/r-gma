/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

/**
 * Time units.
 */
public enum TimeUnit {

	/** Seconds. */
	SECONDS(1),

	/** Minutes. */
	MINUTES(60),

	/** Hours. */
	HOURS(3600),

	/** Days. */
	DAYS(86400);

	private int m_ratio;

	private TimeUnit(int ratio) {
		m_ratio = ratio;
	}

	/**
	 * Gets the number of seconds in this unit.
	 * 
	 * @return The number of seconds in one of this unit.
	 */
	 int getSecs() {
		return m_ratio;
	}
}
