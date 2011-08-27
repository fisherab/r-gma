/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.util.ArrayList;
import java.util.List;

/**
 * A client uses an on-demand producer to publish data into R-GMA when the cost of creating each message is high. The
 * on-demand producer only generates messages when there is a specific query from a consumer.
 */
public class OnDemandProducer extends Producer {

	private String m_hostName;
	private int m_port;
	private List<Table> m_tables = new ArrayList<Table>();

	/**
	 * Creates an on-demand producer.
	 * 
	 * @param hostName
	 *            the host name of the system that will respond to queries
	 * @param port
	 *            the port on the specified host that will respond to queries
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public OnDemandProducer(String hostName, int port) throws RGMAPermanentException, RGMATemporaryException {
		m_servletName = "OnDemandProducerServlet";
		ServletConnection connection = new ServletConnection(m_servletName);
		connection.addParameter("hostName", hostName);
		connection.addParameter("port", port);
		Tuple tuple = connection.sendNonResourceCommand("createOnDemandProducer").getData().get(0);
		m_resourceId = tuple.getInt(0);
		m_hostName = hostName;
		m_port = port;
	}

	/**
	 * Declares a table into which this producer can publish. A subset of a table can be declared using a predicate.
	 * 
	 * @param name
	 *            the name of the table to publish into
	 * @param predicate
	 *            an SQL WHERE clause defining the subset of a table that this producer will publish. To publish to the
	 *            whole table, an empty predicate can be used.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void declareTable(String name, String predicate) throws RGMAPermanentException, RGMATemporaryException {
		try {
			doDeclareTable(name, predicate);
		} catch (UnknownResourceException e) {
			try {
				restore();
				doDeclareTable(name, predicate);
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
		m_tables.add(new Table(name, predicate));
	}

	private void doDeclareTable(String name, String predicate) throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("tableName", name);
		connection.addParameter("predicate", predicate);
		checkOK(connection.sendCommand("declareTable"));
	}

	private void restore() throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		OnDemandProducer p;
		p = new OnDemandProducer(m_hostName, m_port);
		for (Table t : m_tables) {
			p.doDeclareTable(t.m_name, t.m_predicate);
		}
		m_resourceId = p.m_resourceId;
	}

	private class Table {

		private String m_name;
		private String m_predicate;

		private Table(String name, String predicate) {
			m_name = name;
			m_predicate = predicate;
		}

	}

}
