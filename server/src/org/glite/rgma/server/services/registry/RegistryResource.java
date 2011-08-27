/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.registry;

import org.glite.rgma.server.system.ResourceEndpoint;

/**
 * Class which describes a resource data that would be registered in the resources table of a registry
 */
class RegistryResource extends ReplicationTableEntry {
	private String m_predicate;
	private int m_terminationIntervalSecs = 0;
	protected boolean m_isHistory;
	protected boolean m_isLatest;
	protected boolean m_isStatic;
	protected boolean m_isContinuous;
	protected boolean m_isSecondary;
	private String m_tableName;

	protected RegistryResource(String tableName, ResourceEndpoint endpoint, String predicate, int terminationIntervalSecs) {
		super(endpoint);
		m_tableName = tableName;
		m_predicate = predicate;
		m_terminationIntervalSecs = terminationIntervalSecs;
	}

	/**
	 * get the predicate for this resource
	 * 
	 * @return the predicate of this resource
	 */
	String getPredicate() {
		return m_predicate;
	}

	/**
	 * get the table name that this resource publishes to
	 * 
	 * @return the table name this resource publishes to
	 */
	String getTableName() {
		return m_tableName;
	}

	/**
	 * get the termination time in seconds for this resource
	 * 
	 * @return the termination time in seconds
	 */
	int getTerminationIntervalSecs() {
		return m_terminationIntervalSecs;
	}

	/**
	 * is this resource continuous
	 * 
	 * @return true if this resource is latest
	 */
	boolean isContinuous() {
		return m_isContinuous;
	}

	/**
	 * is this resource History
	 * 
	 * @return true if this resource is History
	 */
	boolean isHistory() {
		return m_isHistory;
	}

	/**
	 * is this resource latest
	 * 
	 * @return true if this resource is latest
	 */
	boolean isLatest() {
		return m_isLatest;
	}

	/**
	 * is this resource secondary. Only valid for producers
	 * 
	 * @return true if this resource is secondary
	 */
	boolean isSecondary() {
		return m_isSecondary;
	}

	/**
	 * is this resource static
	 * 
	 * @return true if this resource is static
	 */
	boolean isStatic() {
		return m_isStatic;
	}

	@Override
	String plusOrMinus() {
		return "+";
	}
}
