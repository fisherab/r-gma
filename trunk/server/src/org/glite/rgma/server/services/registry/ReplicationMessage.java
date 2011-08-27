package org.glite.rgma.server.services.registry;

import java.util.List;

public class ReplicationMessage {

	private String m_vdbName;
	private String m_hostName;
	private int m_currentReplicaTimeStamp;
	private int m_previousReplicaTimeStamp;
	private List<RegistryProducerResource> m_ps;
	private List<RegistryConsumerResource> m_cs;
	private List<UnregisterProducerTableRequest> m_ups;
	private List<UnregisterConsumerResourceRequest> m_ucs;

	public ReplicationMessage(String vdbName, String hostName, int currentReplicaTimeStamp, int previousReplicaTimeStamp,
			List<RegistryProducerResource> ps, List<RegistryConsumerResource> cs, List<UnregisterProducerTableRequest> ups,
			List<UnregisterConsumerResourceRequest> ucs) {
		m_vdbName = vdbName;
		m_hostName = hostName;
		m_currentReplicaTimeStamp = currentReplicaTimeStamp;
		m_previousReplicaTimeStamp = previousReplicaTimeStamp;
		m_ps = ps;
		m_cs = cs;
		m_ups = ups;
		m_ucs = ucs;
	}

	public List<RegistryConsumerResource> getConsumers() {
		return m_cs;
	}

	public List<UnregisterConsumerResourceRequest> getConsumersToDelete() {
		return m_ucs;
	}

	public int getCurrentReplicaTimeStamp() {
		return m_currentReplicaTimeStamp;
	}

	public String getHostName() {
		return m_hostName;
	}

	public int getPreviousReplicaTimeStamp() {
		return m_previousReplicaTimeStamp;
	}

	public List<RegistryProducerResource> getProducers() {
		return m_ps;
	}

	public List<UnregisterProducerTableRequest> getProducersToDelete() {
		return m_ups;
	}

	public String getVdbName() {
		return m_vdbName;
	}

	public String toString() {
		return "RM for " + m_vdbName + " from " + m_hostName + " " + m_currentReplicaTimeStamp + "-" + m_previousReplicaTimeStamp + " (P="
				+ m_ps.size() + " C=" + m_cs.size() + " UP=" + m_ups.size() + " UC=" + m_ucs.size() + ")";
	}

}
