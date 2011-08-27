package org.glite.rgma.server.services.registry;

import org.glite.rgma.server.system.ResourceEndpoint;

public class UnregisterConsumerResourceRequest extends ReplicationTableEntry {

	public UnregisterConsumerResourceRequest(ResourceEndpoint endpoint) {
		super(endpoint);
	}

	@Override
	String plusOrMinus() {
		return "-";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof UnregisterConsumerResourceRequest) {
			UnregisterConsumerResourceRequest e = (UnregisterConsumerResourceRequest) obj;
			return e.getEndpoint().equals(m_endpoint);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return getEndpoint().hashCode();
	}

}
