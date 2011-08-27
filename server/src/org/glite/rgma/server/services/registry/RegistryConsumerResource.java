/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.registry;

import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.ResourceEndpoint;

class RegistryConsumerResource extends RegistryResource {

	RegistryConsumerResource(String tableName, ResourceEndpoint endpoint, String predicate, int terminationIntervalSecs, QueryProperties props,
			boolean isSecondary) {
		super(tableName, endpoint, predicate, terminationIntervalSecs);

		if (props != null) {
			m_isContinuous = props.isContinuous();
			m_isHistory = props.isHistory();
			m_isLatest = props.isLatest();
			m_isStatic = props.isStatic();
			m_isSecondary = isSecondary;
		}
	}
}
