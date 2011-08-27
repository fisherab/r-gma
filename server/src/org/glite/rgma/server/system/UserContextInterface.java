package org.glite.rgma.server.system;

import java.util.List;

import org.glite.voms.FQAN;

public interface UserContextInterface {

	public String getDN();

	public List<FQAN> getFQANs();

	public String getHostName();
}
