/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * Attributes of a Producer.
 */
public final class ProducerType {

    /** Is the producer a secondary producer? */
    private final boolean m_secondary;

    /** Does the producer support continuous queries? */
    private final boolean m_continuous ;

    /** Does the producer support static queries? */
    private final boolean m_static;

    /** Does the producer support history queries? */
    private final boolean m_history;

    /** Does the producer support latest queries? */
    private final boolean m_latest;
    
     /**
     * Creates a new ProducerType instance.
     *
     * @param history Supports HISTORY queries.
     * @param latest Supports LATEST queries.
     * @param continuous Supports CONTINUOUS queries.
     * @param isStatic Supports STATIC queries.
     * @param secondaryProducer Is a secondary producer.
     */
    public ProducerType(boolean history, boolean latest, boolean continuous, boolean isStatic, boolean secondary) {
        m_history = history;
        m_latest = latest;
        m_continuous = continuous;
        m_static = isStatic;
        m_secondary = secondary;
    }

    /**
     * Is the producer a secondary producer?
     *
     * @return <code>true</code> if this producer is a secondary producer, <code>false</code> otherwise.
     */
    public boolean isSecondary() {
        return m_secondary;
    }

    /**
     * Does the producer support continuous queries?
     *
     * @return <code>true</code> if this producer supports continuous queries, <code>false</code> otherwise.
     */
    public boolean isContinuous() {
        return m_continuous;
    }

    /**
     * Does the producer support static queries?
     *
     * @return <code>true</code> if this producer supports static queries, <code>false</code> otherwise.
     */
    public boolean isStatic() {
        return m_static;
    }

    /**
     * Does the producer support history queries?
     *
     * @return <code>true</code> if this producer supports history queries, <code>false</code> otherwise.
     */
    public boolean isHistory() {
        return m_history;
    }

    /**
     * Does the producer support latest queries?
     *
     * @return <code>true</code> if this producer supports latest queries, <code>false</code> otherwise.
     */
    public boolean isLatest() {
        return m_latest;
    }

    /**
     * Returns a String representation of this object.
     */
    public String toString() {
        StringBuffer str = new StringBuffer("[");
        str.append(m_continuous ? 'C' : '-');
        str.append(m_history ? 'H' : '-');
        str.append(m_latest ? 'L' : '-');
        str.append(m_secondary ? '2' : '-');
        str.append(m_static ? 'S' : '-');
        str.append(']');

        return str.toString();
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof ProducerType)) {
                return false;
        }
        ProducerType type = (ProducerType) obj;
        if ((m_secondary != type.m_secondary) ||
            (m_continuous != type.m_continuous) ||
            (m_static != type.m_static) ||
            (m_history != type.m_history) ||
            (m_latest != type.m_latest)) {
            return false;
        }
        return true;
    }

    public int hashCode()
    {
        int ret = 0;
        if (m_continuous) ret += 1;
        if (m_latest) ret += 2;
        if (m_history) ret += 4;
        if (m_static) ret += 8;
        if (m_secondary) ret += 16;
        return ret;
    }
}
