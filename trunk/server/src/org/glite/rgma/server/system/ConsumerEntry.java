/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * A consumer entry from the Registry. TODO This class is redundant
 */
public final class ConsumerEntry {

	/**
	 * Consumer's endpoint.
	 */
	private final ResourceEndpoint m_endpoint;

	/**
	 * Creates a new ConsumerEntry
	 */
	public ConsumerEntry(ResourceEndpoint endpoint) {
		m_endpoint = endpoint;
	}

	/**
	 * @return Returns the consumer's endpoint
	 */
	public ResourceEndpoint getEndpoint() {
		return m_endpoint;
	}

	/**
	 * @see Object#toString
	 */
	public String toString() {
		return m_endpoint.toString();
	}

	/**
	 * Compares the specified object with this consumer entry.
	 * 
	 * @param obj
	 *            Object to compare.
	 * @return <code>true</code> if the object is equal.
	 */
	public boolean equals(Object obj) {
		if (!(obj instanceof ConsumerEntry)) {
			return false;
		}

		ConsumerEntry ce = (ConsumerEntry) obj;
		if (!m_endpoint.equals(ce.m_endpoint)) {
			return false;
		}

		return true;
	}

	public int hashCode() {
		return m_endpoint.hashCode();
	}
}
