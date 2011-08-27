/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma;

import java.util.ArrayList;
import java.util.List;

/**
 * A registry provides information about available producers.
 */
public class Registry extends VDB {

	/**
	 * Constructs a registry object for the specified VDB.
	 * 
	 * @param vdbName
	 *            name of VDB
	 */
	public Registry(String vdbName) {
		m_servletName = "RegistryServlet";
		m_vdbName = vdbName;
	}

	/**
	 * Returns a list of all producers for a table with their registered information.
	 * 
	 * @param name
	 *            name of table
	 * 
	 * @return a list of producer table entry objects, one for each producer
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public List<ProducerTableEntry> getAllProducersForTable(String name) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("canForward", true);
		connection.addParameter("tableName", name);
		ArrayList<ProducerTableEntry> pdl = new ArrayList<ProducerTableEntry>();
		for (Tuple t : connection.sendNonResourceCommand("getAllProducersForTable").getData()) {
			ResourceEndpoint endpoint = new ResourceEndpoint(t.getString(0), t.getInt(1));
			ProducerTableEntry pd = new ProducerTableEntry(endpoint, t.getBoolean(2), t
					.getBoolean(3), t.getBoolean(4), t.getBoolean(5), t.getBoolean(6), t.getString(7), t.getInt(8));
			pdl.add(pd);
		}
		return pdl;
	}
}
