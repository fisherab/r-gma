package org.glite.rgma.server.system;

import java.util.List;

import org.glite.voms.FQAN;

public class UserSystemContext extends UserContext implements SystemContextInterface, UserContextInterface {

	private String m_connectingHostName;

	public UserSystemContext(String DN, List<FQAN> fqans, String usersHostName, String connectingHostName) {
		super(DN, fqans, usersHostName);
		m_connectingHostName = connectingHostName;
	}

	/** The host making the connection to the server. */
	public String getConnectingHostName() {
		return m_connectingHostName;
	}
}
