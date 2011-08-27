/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.mediator;

import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.ProducerTableEntry;

import java.util.Map;
import java.util.HashMap;

/**
 * Details of a producer.
 */
public class ProducerDetails {
	/**
	 * The producer's endpoint.
	 */
	private ResourceEndpoint m_endpoint;

	/**
	 * Producer's type.
	 */
	private ProducerType m_producerType;

	/**
	 * Producer's declared tables
	 */
	private Map<String, DeclaredTable> m_tables;

	/**
	 * Creates a new ProducerDetails object from a ProducerTableEntry.
	 */
	public ProducerDetails(ProducerTableEntry producerTable) {
		m_endpoint = producerTable.getEndpoint();
		m_producerType = producerTable.getProducerType();
		m_tables = new HashMap<String, DeclaredTable>();
		m_tables.put(producerTable.getVdbTableName(), new DeclaredTable(producerTable.getVdbTableName(), producerTable.getHistoryRetentionPeriod(), producerTable.getPredicate()));
	}

	/**
	 * Creates a new ProducerDetails object.
	 */
	public ProducerDetails(ResourceEndpoint endpoint, ProducerType producerType, Map<String, DeclaredTable> tables) {
		m_endpoint = endpoint;
		m_producerType = producerType;
		m_tables = tables;
	}

	/**
	 * Compares the specified object with this producer.
	 * 
	 * If obj is a ProducerDetails object, test if it represents the same producer (i.e. has the
	 * same ResourceEndpoint).
	 * 
	 * @param obj
	 *            Object to compare.
	 * @return <code>true</code> if the object is equal to this producer.
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ProducerDetails)) {
			return false;
		}

		ProducerDetails pd = (ProducerDetails) obj;

		if (!m_endpoint.equals(pd.m_endpoint)) {
			return false;
		}

		if (m_producerType == null && pd.m_producerType != null) {
			return false;
		}

		if (m_producerType != null && !m_producerType.equals(pd.m_producerType)) {
			return false;
		}

		return true;
	}

	/**
	 * Gets the producer's endpoint.
	 * 
	 * @return An Endpoint object.
	 */
	public ResourceEndpoint getEndpoint() {
		return m_endpoint;
	}

	/**
	 * Returns the type of the producer.
	 */
	public ProducerType getProducerType() {
		return m_producerType;
	}

	/**
	 * Returns the producer's history retention period.
	 */
	public Map<String, DeclaredTable> getTables() {
		return m_tables;
	}

	@Override
	public int hashCode() {
		int code = m_endpoint.hashCode();
		if (m_producerType != null) {
			code = +m_producerType.hashCode();
		}
		return code;
	}

	/**
	 * @see Object#toString
	 */
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		str.append(m_endpoint);
		if (m_producerType == null) {
			str.append(" for directed query");
		} else {
			str.append(" ").append(m_producerType);
			if (m_tables != null) {
				for (DeclaredTable dt : m_tables.values()) {
					str.append(" ").append(dt);
				}
			}
		}
		return str.toString();
	}
}
