/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * Encapsulates a time value and the units being used.
 */
public final class TimeInterval {

    /** Value in ms. */
    private final long m_value;

    /** Units used to specify interval. */
    private final Units m_units;

    /**
     * Creates a new TimeInterval object.
     *
     * @param value The number of time units.
     * @param units The time units: MILLIS, SECONDS, MINUTES, HOURS, DAYS.
     */
    public TimeInterval(long value, Units units) {
        // Store as ms
        m_value = value * units.getNumMillis();
        m_units = units;
    }

    /**
     * Gets the length of the time interval in the specified units.
     *
     * @param units The time units: MILLIS, SECONDS, MINUTES, HOURS, DAYS.
     *
     * @return The time interval in the given units.
     */
    public long getValueAs(Units units) {
        return m_value / units.getNumMillis();
    }

    /**
     * Compares this object with the specified object.
     *
     * @param obj Object to compare.
     *
     * @return <code>true</code> if this object and the specified object are
     *         equal.
     *
     * @see Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof TimeInterval)) {
            return false;
        }

        TimeInterval ti = (TimeInterval) obj;

        return (ti.m_value == m_value);
    }

    /**
     * Returns a hash code value for the object.
     *
     * @return A hash code value for this object.
     *
     * @see Object#hashCode()
     */
    public int hashCode() {
        return (int) (m_value % Integer.MAX_VALUE);
    }

    /**
     * Returns a string representation of the object.
     *
     * @return A string representation of the object.
     *
     * @see Object#toString()
     */
    public String toString() {
        return getValueAs(m_units) + " " + m_units.toString();
    }
}
