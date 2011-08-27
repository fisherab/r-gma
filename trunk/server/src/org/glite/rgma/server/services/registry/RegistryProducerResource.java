/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.registry;

import org.glite.rgma.server.services.sql.ProducerPredicate;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;

/**
 * data class which will represent a Producer resource
 */
class RegistryProducerResource extends RegistryResource {

	/**
	 * the parsed predicate
	 */
	private ProducerPredicate m_producerPredicate;

	/**
	 * the history retention period in seconds
	 */
	private int m_hrpSecs;

	RegistryProducerResource(String tableName, ResourceEndpoint endpoint, String predicate, int terminationIntervalSecs, ProducerType type, int hrpSecs)
			throws RGMAPermanentException {
		super(tableName, endpoint, predicate, terminationIntervalSecs);

		if (type != null) {
			m_isContinuous = type.isContinuous();
			m_isHistory = type.isHistory();
			m_isLatest = type.isLatest();
			m_isStatic = type.isStatic();
			m_isSecondary = type.isSecondary();
		}
		m_hrpSecs = hrpSecs;

		try {
			if (predicate.equals("") || predicate.toLowerCase().indexOf("where") >= 0) {
				m_producerPredicate = ProducerPredicate.parse(predicate);
			} else {
				m_producerPredicate = ProducerPredicate.parse("WHERE " + predicate);
			}
		} catch (ParseException e) {
			throw new RGMAPermanentException("Error parsing producer predicate " + e.getMessage());
		}
	}

	/**
	 * get the history retention period for this resource
	 * 
	 * @return the history retention period in seconds for this resource
	 */
	int getHrpSecs() {
		return m_hrpSecs;
	}

	/**
	 * Get the parsed producer predicate
	 * 
	 * @return the parsed producer predicate
	 */
	ProducerPredicate getProducerPredicate() {
		return m_producerPredicate;
	}
}
