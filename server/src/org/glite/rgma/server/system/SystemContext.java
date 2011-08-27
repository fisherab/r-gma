package org.glite.rgma.server.system;

public class SystemContext implements SystemContextInterface {

	private String m_hostName;

	public SystemContext(String hostName) {

		m_hostName = hostName;
	}

	public String getConnectingHostName() {
		return m_hostName;
	}

}
