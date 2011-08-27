package org.glite.rgma.server.services.registry;

import org.glite.rgma.server.system.ResourceEndpoint;

public class UnregisterProducerTableRequest extends ReplicationTableEntry {

	private String m_tableName;

	public UnregisterProducerTableRequest(String tableName, ResourceEndpoint endpoint) {
		super(endpoint);
		m_tableName = tableName;
	}

	public String getTableName() {
		return m_tableName;
	}

	@Override
	String plusOrMinus() {
		return "-";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof UnregisterProducerTableRequest) {
			UnregisterProducerTableRequest e = (UnregisterProducerTableRequest) obj;
			return e.getEndpoint().equals(m_endpoint) && e.getTableName().equals(m_tableName);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return getEndpoint().hashCode();
	}

}
