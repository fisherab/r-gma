/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;


/**
 * Time units.
 */
public class Units {
    /** Milliseconds. */
    protected static final int MS = 0;

    /** Seconds. */
    protected static final int S = 1;

    /** Minutes. */
    protected static final int M = 2;

    /** Hours. */
    protected static final int H = 3;

    /** Days. */
    protected static final int D = 4;

    /** Milliseconds. */
    public static final Units MILLIS = new Units(MS);

    /** Seconds. */
    public static final Units SECONDS = new Units(S);

    /** Minutes. */
    public static final Units MINUTES = new Units(M);

    /** Hours. */
    public static final Units HOURS = new Units(H);

    /** Days. */
    public static final Units DAYS = new Units(D);

    /** Number of milliseconds in 1ms, 1sec, 1 minute, 1 hour, 1 day. */
    private static final long[] RATIOS = { 1, 1000, 60000, 3600000, 86400000 };

    /** Names of units. */
    private static final String[] NAMES = { "millis", "seconds", "minutes", "hours", "days" };

    /** Units (MS, S, M, H or D). */
    private int m_units;

    /**
     * Constructs a Units object.
     *
     * @param units An <code>int</code> representing the units to use.
     */
    protected Units(int units) {
        m_units = units;
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
        return (obj instanceof Units) && (((Units) obj).getUnits() == getUnits());
    }

    /**
     * Returns a hash code value for the object.
     *
     * @return a hash code value for this object.
     *
     * @see Object#hashCode()
     */
    public int hashCode() {
        return m_units;
    }

    /**
     * Gets the number of milliseconds in this unit.
     *
     * @return The number of milliseconds in one of this unit.
     */
    protected long getNumMillis() {
        return RATIOS[m_units];
    }

    /**
     * Gets the units represented by this Units object.
     *
     * @return An <code>int</code> representing the units, one of
     *         S (second), M (minute), H (hour) or D (day).
     */
    protected int getUnits() {
        return m_units;
    }

    /**
     * Returns a string representation of the object.
     *
     * @return A string representation of the object.
     *
     * @see Object#toString()
     */
    public String toString() {
        return NAMES[m_units];
    }
}
