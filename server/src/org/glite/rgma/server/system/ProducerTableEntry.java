/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * A producer-table entry from the registry. Contains information about a single table declared by a producer.
 */
public final class ProducerTableEntry {

	/** The producer's endpoint. */
	private final ResourceEndpoint m_endpoint;

	/** History retention period in seconds */
	private final int m_hrpSec;

	/** The producer's predicate for this table. */
	private final String m_predicate;

	/** Producer's type. */
	private final ProducerType m_producerType;

	/** Table name. */
	private final String m_tableName;

	/** The producer's VDB. */
	private String m_vdbName;

	public ProducerTableEntry(ResourceEndpoint endpoint, String vdbName, String tableName, ProducerType producerType, int hrpSec, String predicate) {
		m_producerType = producerType;
		m_endpoint = endpoint;
		m_hrpSec = hrpSec;
		m_tableName = tableName;
		m_predicate = predicate;
		m_vdbName = vdbName;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ProducerTableEntry)) {
			return false;
		}

		ProducerTableEntry pd = (ProducerTableEntry) obj;

		if (!m_endpoint.equals(pd.m_endpoint)) {
			return false;
		}

		if (!m_producerType.equals(pd.m_producerType)) {
			return false;
		}

		if (m_hrpSec != pd.m_hrpSec) {
			return false;
		}

		if (!m_tableName.equals(pd.m_tableName)) {
			return false;
		}

		if (!m_predicate.equals(pd.m_predicate)) {
			return false;
		}

		if (!m_vdbName.equals(pd.m_vdbName)) {
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
	 * Returns the producer's history retention period.
	 */
	public int getHistoryRetentionPeriod() {
		return m_hrpSec;
	}

	/**
	 * Returns the producer's predicate for this table.
	 */
	public String getPredicate() {
		return m_predicate;
	}

	/**
	 * Returns the type of the producer.
	 */
	public ProducerType getProducerType() {
		return m_producerType;
	}

	/**
	 * Returns the tableName.
	 */
	public String getTableName() {
		return m_tableName;
	}

	/**
	 * Returns the producer's vdb.
	 */
	public String getVdbName() {
		return m_vdbName;
	}

	/**
	 * Returns the producer's vdbTableName, in upper case. If the vdbName has not been set it uses "DEFAULT"
	 */
	public String getVdbTableName() {
		return (m_vdbName + "." + m_tableName).toUpperCase();
	}

	@Override
	public int hashCode() {
		return m_endpoint.hashCode() + m_producerType.hashCode() + (m_hrpSec >>> 32) + m_tableName.hashCode() + m_predicate.hashCode() + m_vdbName.hashCode();
	}

	/**
	 * @see Object#toString
	 */
	@Override
	public String toString() {
		return "PTE[" + "endpoint=" + m_endpoint + ", type=" + m_producerType + ", hrp=" + m_hrpSec + ", table name=" + m_tableName + ", predicate="
				+ m_predicate + ", vdbName=" + m_vdbName + "]";
	}
}
