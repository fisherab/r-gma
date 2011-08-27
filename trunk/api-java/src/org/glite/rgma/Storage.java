/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma;

/**
 * Storage location for tuples.
 */
public class Storage {

	private enum MD {
		M, D
	};

	/** Logical name of storage location. */
	private String m_logicalName;

	/** Type of storage. */
	private MD m_type;

	private Storage(MD type) {
		m_type = type;
	}

	private Storage(String logicalName) {
		m_type = MD.D;
		m_logicalName = logicalName;
	}

	/**
	 * Gets a permanent named database storage object. This storage can be reused.
	 * 
	 * @param logicalName
	 *            the logical name of the storage system. If it has been used before the storage
	 *            will be reused. The DN of the user is used in conjunction with the logical name as
	 *            a key to the physical storage so that different users cannot share the same
	 *            storage.
	 * 
	 * @return a permanent named database storage object
	 */
	public static Storage getDatabaseStorage(String logicalName) {
		return new Storage(logicalName);
	}

	/**
	 * Gets a temporary database storage object. This storage cannot be reused.
	 * 
	 * @return a temporary database storage object
	 */
	public static Storage getDatabaseStorage() {
		return new Storage(MD.D);
	}

	/**
	 * Gets a temporary memory storage object. This storage cannot be reused.
	 * 
	 * @return a temporary memory storage object
	 */
	public static Storage getMemoryStorage() {
		return new Storage(MD.M);
	}

	/**
	 * Determines if this storage system is a database.
	 * 
	 * @return True if the storage system is a database.
	 */
	boolean isDatabase() {
		return m_type == MD.D;
	}

	/**
	 * Determines if this storage system is memory.
	 * 
	 * @return True if the storage system is memory.
	 */
	boolean isMemory() {
		return m_type == MD.M;
	}

	/**
	 * Gets this Storage's logical name.
	 * 
	 * @return the logical name of the storage
	 */
	String getLogicalName() {
		return m_logicalName;
	}
}
