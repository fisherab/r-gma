/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * The properties of a consumer query.  The following static methods and
 * constants are available:
 *
 * <ul>
 * <li>
 * HISTORY: <code>QueryProperties.HISTORY</code>
 * </li>
 * <li>
 * HISTORY over a time interval: <code>QueryProperties.getHistory(TimeInterval)</code>
 * </li>
 * <li>
 * LATEST: <code>QueryProperties.LATEST</code>
 * </li>
 * <li>
 * LATEST over a time interval: <code>QueryProperties.getLatest(TimeInterval)</code>
 * </li>
 * <li>
 * CONTINUOUS: <code>QueryProperties.CONTINUOUS</code>
 * </li>
 * <li>
 * CONTINUOUS over a time interval: <code>QueryProperties.getContinuous(TimeInterval)</code>
 * </li>
 * </ul>
 */
public class QueryProperties {
    /** Continuous type. */
    protected static final int C = 0;

    /** Latest type. */
    protected static final int L = 1;

    /** History type. */
    protected static final int H = 2;

    /** Static type. */
    protected static final int S = 3;

    /** Stream data continuously, starting with the next inserted tuple. */
    public static final QueryProperties CONTINUOUS = new QueryProperties(C);

    /** Retrieve data from LATEST producers. */
    public static final QueryProperties LATEST = new QueryProperties(L);

    /** Retrieve data from HISTORY producers. */
    public static final QueryProperties HISTORY = new QueryProperties(H);

    /** Retrieve data from STATIC producers. */
    public static final QueryProperties STATIC = new QueryProperties(S);

    /** QueryProperties names. */
    protected static final String[] QUERYTYPE_NAME = { "Continuous", "Latest", "History", "Static" };

    /** Time interval for this query. */
    private TimeInterval m_timeInterval;

    /** Type of this query as an integer. */
    private int m_type;

    /**
     * Creates a new QueryProperties object of type <code>type</code> with a time interval.
     *
     * @param type Query type.
     * @param timeInterval The period over which to retrieve data.
     */
    protected QueryProperties(int type, TimeInterval timeInterval) {
        this(type);
        m_timeInterval = timeInterval;
    }

    /**
     * Constructs a QueryProperties object from an int.
     *
     * @param type The type of the query: C, L, S or H
     */
    protected QueryProperties(int type) {
        m_type = type;
    }

    /**
     * Creates a QueryProperties object from another QueryProperties object.
     *
     * @param query Another QueryProperties object.
     */
    protected QueryProperties(QueryProperties query) {
        this(query.m_type, query.m_timeInterval);
    }

    /**
     * Returns a CONTINUOUS QueryProperties object with a time interval.
     *
     * @param timeInterval Time interval for query.
     *
     * @return A QueryProperties object.
     */
    public static final QueryProperties getContinuous(TimeInterval timeInterval) {
        return new QueryProperties(C, timeInterval);
    }

    /**
     * Returns a HISTORY QueryProperties object with a time interval.
     *
     * @param timeInterval Time interval for query.
     *
     * @return A QueryProperties object.
     */
    public static final QueryProperties getHistory(TimeInterval timeInterval) {
        return new QueryProperties(H, timeInterval);
    }

    /**
     * Returns a LATEST QueryProperties object with a time interval.
     *
     * @param timeInterval Time interval for query.
     *
     * @return A QueryProperties object.
     */
    public static final QueryProperties getLatest(TimeInterval timeInterval) {
        return new QueryProperties(L, timeInterval);
    }

    /**
     * Determines if this is a CONTINUOUS query.
     *
     * @return true if a CONTINUOUS query, otherwise false.
     */
    public boolean isContinuous() {
        return m_type == C;
    }

    /**
     * Determines if this is a HISTORY query.
     *
     * @return true if a HISTORY query, otherwise false.
     */
    public boolean isHistory() {
        return m_type == H;
    }

    /**
     * Determines if this is a LATEST query.
     *
     * @return true if a LATEST query, otherwise false.
     */
    public boolean isLatest() {
        return m_type == L;
    }

    /**
     * Determines if this is a STATIC query.
     *
     * @return true if a STATIC query, otherwise false.
     */
    public boolean isStatic() {
        return m_type == S;
    }

    /**
     * Returns the time interval associated with this QueryProperties.
     *
     * @return The time interval associated with this QueryProperties, or
     * <code>null</code> if no time interval has been set.
     */
    public TimeInterval getTimeInterval() {
        return m_timeInterval;
    }

    /**
     * Determines if this QueryProperties has an associated TimeInterval.
     *
     * @return true if it has a TimeInterval, otherwise false.
     */
    public boolean hasTimeInterval() {
        return (m_timeInterval != null);
    }

    /**
     * Gets the query type.
     *
     * @return The query type: CONTINUOUS, LATEST, STATIC or HISTORY
     */
    protected int getType() {
        return m_type;
    }

    /**
     * Returns a string representation of the object.
     *
     * @return A string representation of the object.
     *
     * @see Object#toString()
     */
    public String toString() {
        return QUERYTYPE_NAME[m_type] + (hasTimeInterval() ? ("[" + m_timeInterval + "]") : "");
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
        if (!(obj instanceof QueryProperties)) {
                return false;
        }
        QueryProperties props = (QueryProperties) obj;
        if (m_type != props.m_type) {
            return false;
        }
        if ((m_timeInterval == null) && (props.m_timeInterval != null)) {
            return false;
        }
        if ((m_timeInterval != null) && !m_timeInterval.equals(props.m_timeInterval)) {
            return false;
        }

        return true;
    }

    /**
     * Returns a hash code value for the object.
     *
     * @return A hash code value for this object.
     *
     * @see Object#hashCode()
     */
    public int hashCode() {
        return (m_type + "//" + m_timeInterval).hashCode();
    }
}
