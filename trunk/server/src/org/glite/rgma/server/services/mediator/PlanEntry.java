/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.mediator;

import org.glite.rgma.server.services.sql.SelectStatement;

/**
 * Details of a producer a consumer should contact to answer it's query
 */
public class PlanEntry {
	
    private final ProducerDetails m_producer;
    private final SelectStatement m_select;

    /**
     * Constructor.
     *
     * @param producer Producer to contact
     * @param select SQL SELECT query.
     */
    public PlanEntry(ProducerDetails producer, SelectStatement select) {
        m_producer = producer;
        m_select = select;
    }

    /**
     * Get the endpoint of the producer associated with this plan entry.
     */
    public ProducerDetails getProducer() {
        return m_producer;
    }

    /**
     * Get the parsed SQL SELECT statement associated with this plan entry.
     */
    public SelectStatement getSelect() {
        return m_select;
    }

    /**
     * Compares the specified object with this plan entry.
     *
     * If obj is a PlanEntry object, test if it represents the same
     * producer and the same query.
     *
     * @param obj Object to compare.
     * @return <code>true</code> if the object is equal to this plan entry.
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof PlanEntry)) {
            return false;
        }

        PlanEntry entry = (PlanEntry) obj;

        if (!m_producer.equals(entry.m_producer)) {
            return false;
        }

        if (!m_select.equals(entry.m_select)) {
            return false;
        }

        return true;
    }

    public int hashCode() {
        return m_producer.hashCode() + m_select.hashCode();
    }

    /**
     * @see Object#toString
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("(").append(m_producer);
        str.append("), (").append(m_select);
        str.append(")");
        return str.toString();
    }
}

