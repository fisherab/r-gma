/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.util.ArrayList;
import java.util.List;

/**
 * Republish information from other producers.
 */
public class SecondaryProducer extends Producer {

	private List<Table> m_tables = new ArrayList<Table>();
	private Storage m_storage;
	private SupportedQueries m_supportedQueries;

	/**
	 * Creates a secondary producer to republish information.
	 * 
	 * @param storage
	 *            a storage object to define the type and, if permanent, the name of the storage to be used
	 * @param supportedQueries
	 *            a supported queries object to define what query types the producer will be able to support
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public SecondaryProducer(Storage storage, SupportedQueries supportedQueries) throws RGMAPermanentException, RGMATemporaryException {
		m_servletName = "SecondaryProducerServlet";
		ServletConnection connection = new ServletConnection(m_servletName);
		String type = storage.isDatabase() ? "database" : "memory";
		connection.addParameter("type", type);
		if (storage.getLogicalName() != null) {
			connection.addParameter("logicalName", storage.getLogicalName());
		}
		connection.addParameter("isLatest", supportedQueries.isLatest());
		connection.addParameter("isHistory", supportedQueries.isHistory());
		Tuple tuple = connection.sendNonResourceCommand("createSecondaryProducer").getData().get(0);
		m_resourceId = tuple.getInt(0);
		m_storage = storage;
		m_supportedQueries = supportedQueries;
	}

	/**
	 * Contacts the secondary producer resource to check that it is still alive and prevent it from being timed out.
	 * 
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void showSignOfLife() throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = getNewConnection();
		try {
			checkOK(connection.sendCommand("showSignOfLife"));
		} catch (UnknownResourceException e) {
			try {
				// No need to send a showSignOfLife - just restore
				restore();
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
	}

	/**
	 * Return the resouceId to be used as the argument to subsequent static showSignOfLife calls.
	 * 
	 * @return the resourceId
	 */
	public int getResourceId() {
		return m_resourceId;
	}

	/**
	 * Contacts the secondary producer resource to check that it is still alive and prevent it from being timed out.
	 * 
	 * @param resourceId
	 *            the identifier of the resource to be checked
	 * @return <code>true</code> if resource is still alive otherwise <code>false</code>
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public static boolean showSignOfLife(int resourceId) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection("SecondaryProducerServlet");
		connection.addParameter("connectionId", resourceId);
		try {
			checkOK(connection.sendCommand("showSignOfLife"));
			return true;
		} catch (UnknownResourceException e) {
			return false;
		}
	}

	private class Table {
		private String m_name;
		private String m_predicate;
		private TimeInterval m_historyRetentionPeriod;

		private Table(String name, String predicate, TimeInterval historyRetentionPeriod) {
			m_name = name;
			m_predicate = predicate;
			m_historyRetentionPeriod = historyRetentionPeriod;
		}

	}

	/**
	 * Declares a table, specifying the retention period for history tuples. Tuples will be removed from storage when
	 * the history retention period expires.
	 * 
	 * @param name
	 *            the name of the table to declare
	 * @param predicate
	 *            an SQL WHERE clause defining the subset of a table that this Producer will publish. To publish to the
	 *            whole table, an empty predicate can be used.
	 * @param historyRetentionPeriod
	 *            the history retention period for this table
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void declareTable(String name, String predicate, TimeInterval historyRetentionPeriod) throws RGMAPermanentException, RGMATemporaryException {
		try {
			doDeclareTable(name, predicate, historyRetentionPeriod);
		} catch (UnknownResourceException e) {
			try {
				restore();
				doDeclareTable(name, predicate, historyRetentionPeriod);
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
		m_tables.add(new Table(name, predicate, historyRetentionPeriod));
	}

	private void doDeclareTable(String name, String predicate, TimeInterval historyRetentionPeriod) throws RGMAPermanentException, RGMATemporaryException,
			UnknownResourceException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("tableName", name);
		connection.addParameter("predicate", predicate);
		connection.addParameter("hrpSec", (int) historyRetentionPeriod.getValueAs(TimeUnit.SECONDS));
		checkOK(connection.sendCommand("declareTable"));
	}

	private void restore() throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		SecondaryProducer p;
		p = new SecondaryProducer(m_storage, m_supportedQueries);
		for (Table t : m_tables) {
			p.doDeclareTable(t.m_name, t.m_predicate, t.m_historyRetentionPeriod);
		}
		m_resourceId = p.m_resourceId;
	}
}
