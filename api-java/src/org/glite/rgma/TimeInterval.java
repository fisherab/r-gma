/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

/**
 * Encapsulates a time value and the units being used.
 */
public class TimeInterval {
	/** Value in seconds. */
	private int m_value;

	/**
	 * Creates a new TimeInterval object.
	 * 
	 * @param value
	 *            the number of time units
	 * @param units
	 *            the time units: MILLIS, SECONDS, MINUTES, HOURS, DAYS
	 * @throws RGMAPermanentException
	 *             if the time interval is negative
	 */
	public TimeInterval(int value, TimeUnit units) throws RGMAPermanentException {
		if (value < 0) {
			throw new RGMAPermanentException("Time interval may not be negative");
		}
		if ( value > Integer.MAX_VALUE / units.getSecs() ) {
			throw new RGMAPermanentException("Interval is too large to express as an int in seconds");
		}
		m_value = value * units.getSecs();
	}

	/**
	 * Returns the length of the time interval in the specified units.
	 * 
	 * @param units
	 *            the time units: SECONDS, MINUTES, HOURS, DAYS
	 * @return the time interval in the given units
	 * @throws RGMAPermanentException
	 *             if the interval is too large to express as an int in the chosen units.
	 */
	public int getValueAs(TimeUnit units) throws RGMAPermanentException {
		return m_value / units.getSecs();
	}
}
