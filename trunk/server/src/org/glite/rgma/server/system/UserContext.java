package org.glite.rgma.server.system;

import java.util.List;

import org.glite.voms.FQAN;

public class UserContext implements UserContextInterface {

	private String m_DN;
	private List<FQAN> m_FQANs;
	private String m_hostName;

	public UserContext(String DN, List<FQAN> fqans, String hostName) {
		m_DN = DN;
		m_FQANs = fqans;
		m_hostName = hostName;
	}

	public String getDN() {
		return m_DN;
	}
	
	public List<FQAN> getFQANs() {
		return m_FQANs;
	}

	public String getHostName() {
		return m_hostName;
	}

}
