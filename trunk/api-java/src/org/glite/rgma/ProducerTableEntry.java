/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

/**
 * A producer table entry from the registry.
 */
public class ProducerTableEntry {
	/** The producer's endpoint. */
	private ResourceEndpoint m_endpoint;

	/** Is the producer secondary? */
	private boolean m_secondary;

	/** Does the producer support continuous queries? */
	private boolean m_continuous;

	/** Does the producer support static queries? */
	private boolean m_static;

	/** Does the producer support history queries? */
	private boolean m_history;

	/** Does the producer support latest queries? */
	private boolean m_latest;

	/** Predicate */
	private String m_predicate;
	
	/** History retention period. */
	private TimeInterval m_hrp;


	ProducerTableEntry(ResourceEndpoint endpoint, boolean isSecondary, boolean isContinuous, boolean isStatic, boolean isHistory, boolean isLatest, String predicate, int hrpSec)
			throws RGMAPermanentException {
		m_endpoint = endpoint;
		m_secondary = isSecondary;
		m_continuous = isContinuous;
		m_static = isStatic;
		m_history = isHistory;
		m_latest = isLatest;
		m_predicate = predicate;
		m_hrp = new TimeInterval(hrpSec, TimeUnit.SECONDS);
	}

	/**
	 * Returns the producer's endpoint.
	 * 
	 * @return an endpoint object
	 */
	public ResourceEndpoint getEndpoint() {
		return m_endpoint;
	}

	/**
	 * Return the predicate.
	 * 
	 * @return predicate
	 */
	public String getPredicate() {
		return m_predicate;
	}
	
	/**
	 * Returns the producer's history retention period.
	 * 
	 * @return history retention period as a time interval
	 */
	public TimeInterval getRetentionPeriod() {
		return m_hrp;
	}

	/**
	 * Does the producer support continuous queries?
	 * 
	 * @return <code>true</code> if producer supports continuous queries
	 */
	public boolean isContinuous() {
		return m_continuous;
	}

	/**
	 * Does the producer support history queries?
	 * 
	 * @return <code>true</code> if producer supports history queries
	 */
	public boolean isHistory() {
		return m_history;
	}

	/**
	 * Does the producer support latest queries?
	 * 
	 * @return <code>true</code> if producer supports latest queries
	 */
	public boolean isLatest() {
		return m_latest;
	}
	
	/**
	 * Is the producer a secondary producer?
	 * 
	 * @return <code>true</code> if producer is secondary
	 */
	public boolean isSecondary() {
		return m_secondary;
	}

	/**
	 * Does the producer support static queries?
	 * 
	 * @return <code>true</code> if producer supports static queries
	 */
	public boolean isStatic() {
		return m_static;
	}

	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 * 
	 * @see Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer("ProducerTableEntry[");
		str.append("endpoint=").append(m_endpoint);
		str.append(", type=");
		str.append(m_continuous ? 'C' : '-');
		str.append(m_history ? 'H' : '-');
		str.append(m_latest ? 'L' : '-');
		str.append(m_secondary ? 'R' : '-');
		str.append(m_static ? 'S' : '-');
		str.append(", predicate=").append(m_predicate);
		str.append(", retention period=").append(m_hrp);
		str.append("]");
		return str.toString();
	}
}
