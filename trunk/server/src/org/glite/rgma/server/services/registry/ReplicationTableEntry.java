package org.glite.rgma.server.services.registry;

import org.glite.rgma.server.system.ResourceEndpoint;

public abstract class ReplicationTableEntry {

	abstract String plusOrMinus();
	
	protected ResourceEndpoint m_endpoint;

	public ReplicationTableEntry(ResourceEndpoint endpoint) {
		m_endpoint = endpoint;
	}

	ResourceEndpoint getEndpoint() {
		return m_endpoint;
	}

}
