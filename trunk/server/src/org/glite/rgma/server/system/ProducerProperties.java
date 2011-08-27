/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * Properties of a Producer.
 */
public class ProducerProperties {
    /** Location to store tuples. */
    private Storage m_storage;

    /** Does this producer support history queries? */
    private boolean m_history;

    /** Does this producer support latest queries? */
    private boolean m_latest;
    
    /** Does this producer support static queries? */
	private boolean m_static;

	/** Does this producer support continuous queries? */
	private boolean m_continuous;

    /**
     * Creates a new ProducerProperties object.
     *
     * @param storage The storage location for tuples.
     * @param history Supports history queries.
     * @param latest Supports latest queries.     
     */
    public ProducerProperties(Storage storage, boolean history, boolean latest) {
        m_storage = storage;
        m_history = history;
        m_latest = latest; 
        m_continuous = true;
    }

    /**
     * For the OnDemandProducer
     */
    public ProducerProperties() {
    	m_static = true;
	}

    public boolean isHistory() {
        return m_history;
    }

    public boolean isLatest() {
        return m_latest;
    }
    
    public boolean isStatic() {
        return m_static;
    }
    
    public boolean isContinuous() {
        return m_continuous;
    }

    public Storage getStorage() {
        return m_storage;
    }

    public String getProducerType() {
        return (m_continuous ? "C" : "-") + (m_latest ? "L" : "-") + (m_history ? "H" : "-") + (m_static ? "S" : "-");
    }
    
    /**
     * Returns a String representation of this ProducerProperties object.
     */
    public String toString() {
        return "ProducerProperties[" + ((m_storage == null) ? "" : m_storage) + ", " + getProducerType() + "]";
    }
}
