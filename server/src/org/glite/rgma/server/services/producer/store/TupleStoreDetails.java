/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.Storage.StorageType;

/**
 * Details of a tuple store.
 */
public class TupleStoreDetails {
	/** Type of tuple store. */
	private StorageType m_type;

	/** Logical name. */
	private String m_logicalName;

	/** Supports latest queries. */
	private boolean m_supportsLatest = false;

	/** DN of tuple store owner. */
	private String m_ownerDN;

	/** endpoint of producer creating the store */
	private ResourceEndpoint m_endpoint;

	/**
	 * Creates a new TupleStoreDetails object.
	 */
	public TupleStoreDetails(StorageType type, String logicalName, String ownerDN, boolean latest, ResourceEndpoint endpoint) {
		m_type = type;
		m_ownerDN = ownerDN;
		m_logicalName = logicalName;
		m_supportsLatest = latest;
		m_endpoint = endpoint;
	}

	/**
	 * Creates a new TupleStoreDetails object without a specified endpoint. This object may be
	 * printed, but if you try to access the endpoint an exceptino is thrown. It is expected that
	 * setEndpoint() will be called.
	 */
	public TupleStoreDetails(StorageType type, String logicalName, String ownerDN, boolean latest) {
		m_type = type;
		m_ownerDN = ownerDN;
		m_logicalName = logicalName;
		m_supportsLatest = latest;
	}

	public ResourceEndpoint getEndpoint() throws RGMAPermanentException {
		if (m_endpoint == null) {
			throw new RGMAPermanentException("TupleStoreDetails has not get the endpoint set");
		}
		return m_endpoint;
	}

	/**
	 * Determines if this storage system is a database.
	 * 
	 * @return <code>true</code> if the storage system is a database.
	 */
	public boolean isDatabase() {
		return (m_type == StorageType.DB);
	}

	/**
	 * Determines if this storage system is memory.
	 * 
	 * @return <code>true</code> if the storage system is memory.
	 */
	public boolean isMemory() {
		return (m_type == StorageType.MEM);
	}

	/**
	 * Gets the logical name for this storage system.
	 * 
	 * @return Logical name as a String
	 */
	public String getLogicalName() {
		return m_logicalName;
	}

	/**
	 * Gets the DN of the tuple store owner.
	 * 
	 * @return The DN of the tuple store owner.
	 */
	public String getOwnerDN() {
		return m_ownerDN;
	}

	/**
	 * Determines if this tuple store supports history queries.
	 * 
	 * @return <code>true</code> if this tuple store supports history queries.
	 */
	public boolean supportsHistory() {
		return true;
	}

	/**
	 * Determines if this tuple store supports latest queries.
	 * 
	 * @return <code>true</code> if this tuple store supports latest queries.
	 */
	public boolean supportsLatest() {
		return m_supportsLatest;
	}

	/**
	 * Set supporthistory status to true.
	 * 
	 */
	public void setSupportsLatest() {
		m_supportsLatest = true;
		;
	}

	/**
	 * Gets the type of storage.
	 * 
	 * @return The type of storage (StorageType.DB or StorageType.MEM).
	 */
	public StorageType getType() {
		return m_type;
	}

	/**
	 * @see Object#toString()
	 */
	public String toString() {
		String result = m_logicalName + "/[" + m_ownerDN + "] " + "H" + (m_supportsLatest ? "L" : "-") + " " + (isTemporary() ? "temp" : "perm") + " (" + m_type + ") ";
		if (m_endpoint == null) {
			return result;
		} else {
			return result + m_endpoint.getURL().getPath() + " " + m_endpoint;
		}
	}

	/**
	 * Determines if this tuple store is temporary.
	 * 
	 * @return <code>true</code> if this tuple store is temporary.
	 */
	public boolean isTemporary() {
		return m_logicalName.length() == 0;
	}

	public void setEndpoint(ResourceEndpoint endpoint) throws RGMAPermanentException {
		m_endpoint = endpoint;
	}

}
