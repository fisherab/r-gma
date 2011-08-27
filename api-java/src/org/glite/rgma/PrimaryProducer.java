/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A primary producer publishes information into R-GMA.
 */
public class PrimaryProducer extends Producer {

	private class Table {

		private String m_name;
		private String m_predicate;
		private TimeInterval m_historyRetentionPeriod;
		private TimeInterval m_latestRetentionPeriod;

		private Table(String name, String predicate, TimeInterval historyRetentionPeriod, TimeInterval latestRetentionPeriod) {
			m_name = name;
			m_predicate = predicate;
			m_historyRetentionPeriod = historyRetentionPeriod;
			m_latestRetentionPeriod = latestRetentionPeriod;
		}

	}

	private SupportedQueries m_supportedQueries;
	private Storage m_storage;
	private List<Table> m_tables = new ArrayList<Table>();

	/**
	 * Creates a primary producer that uses the specified data storage and supported queries.
	 * 
	 * @param storage
	 *            a storage object to define the type and, if permanent, the name of the storage to be used
	 * @param supportedQueries
	 *            a supported queries object to define what query types the producer will be able to support.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public PrimaryProducer(Storage storage, SupportedQueries supportedQueries) throws RGMAPermanentException, RGMATemporaryException {
		m_servletName = "PrimaryProducerServlet";
		ServletConnection connection = new ServletConnection(m_servletName);
		String type = storage.isDatabase() ? "database" : "memory";
		connection.addParameter("type", type);
		if (storage.getLogicalName() != null) {
			connection.addParameter("logicalName", storage.getLogicalName());
		}
		connection.addParameter("isLatest", supportedQueries.isLatest());
		connection.addParameter("isHistory", supportedQueries.isHistory());
		Tuple tuple = connection.sendNonResourceCommand("createPrimaryProducer").getData().get(0);
		m_resourceId = tuple.getInt(0);
		m_storage = storage;
		m_supportedQueries = supportedQueries;
	}

	/**
	 * Declares a table, specifying the time intervals for history and latest retention periods. The latest retention
	 * period specifies the time interval relative to the timestamp field of the tuple for which a tuple is valid for a
	 * latest query. After this time interval, the tuple will no longer be returned to latest queries and may be removed
	 * from latest storage. Tuples may be removed from history storage when the history retention period relative to
	 * insertion into the tuple store is exceeded.
	 * 
	 * @param name
	 *            the name of the table to declare
	 * @param predicate
	 *            an SQL WHERE clause defining the subset of a table that this producer will publish. To publish to the
	 *            whole table, an empty predicate can be used.
	 * @param historyRetentionPeriod
	 *            the history retention period for this table
	 * @param latestRetentionPeriod
	 *            the default latest retention period for tuples inserted into this table. It can be overridden when
	 *            inserting tuples.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void declareTable(String name, String predicate, TimeInterval historyRetentionPeriod, TimeInterval latestRetentionPeriod)
			throws RGMAPermanentException, RGMATemporaryException {
		try {
			doDeclareTable(name, predicate, historyRetentionPeriod, latestRetentionPeriod);
		} catch (UnknownResourceException e) {
			try {
				restore();
				doDeclareTable(name, predicate, historyRetentionPeriod, latestRetentionPeriod);
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
		m_tables.add(new Table(name, predicate, historyRetentionPeriod, latestRetentionPeriod));

	}

	private void doDeclareTable(String name, String predicate, TimeInterval historyRetentionPeriod, TimeInterval latestRetentionPeriod)
			throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("tableName", name);
		connection.addParameter("predicate", predicate);
		connection.addParameter("hrpSec", historyRetentionPeriod.getValueAs(TimeUnit.SECONDS));
		connection.addParameter("lrpSec", latestRetentionPeriod.getValueAs(TimeUnit.SECONDS));
		checkOK(connection.sendCommand("declareTable"));
	}

	private void restore() throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		PrimaryProducer p = new PrimaryProducer(m_storage, m_supportedQueries);
		for (Table t : m_tables) {
			p.doDeclareTable(t.m_name, t.m_predicate, t.m_historyRetentionPeriod, t.m_latestRetentionPeriod);
		}
		m_resourceId = p.m_resourceId;
	}

	/**
	 * Publishes a single tuple into a table.
	 * 
	 * @param insertStatement
	 *            a string representing a SQL INSERT statement providing the data to publish and the table into which to
	 *            put it
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void insert(String insertStatement) throws RGMAPermanentException, RGMATemporaryException {
		try {
			doInsert(insertStatement);
		} catch (UnknownResourceException e) {
			try {
				restore();
				doInsert(insertStatement);
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
	}

	private void doInsert(String insertStatement) throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		ServletConnection connection = getNewConnection();
		connection.setRequestMethodPost();
		connection.addParameter("insert", insertStatement);
		checkOK(connection.sendCommand("insert"));
	}

	/**
	 * Publishes a single tuple into a table, overriding the latest retention period.
	 * 
	 * @param insertStatement
	 *            a string representing a SQL INSERT statement providing the data to publish and the table into which to
	 *            put it
	 * @param latestRetentionPeriod
	 *            latest retention period for this tuple (overrides LRP defined for table)
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void insert(String insertStatement, TimeInterval latestRetentionPeriod) throws RGMAPermanentException, RGMATemporaryException {
		try {
			doInsert(insertStatement, latestRetentionPeriod);
		} catch (UnknownResourceException e) {
			try {
				restore();
				doInsert(insertStatement, latestRetentionPeriod);
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
	}

	private void doInsert(String insertStatement, TimeInterval latestRetentionPeriod) throws RGMAPermanentException, RGMATemporaryException,
			UnknownResourceException {
		ServletConnection connection = getNewConnection();
		connection.setRequestMethodPost();
		connection.addParameter("insert", insertStatement);
		connection.addParameter("lrpSec", (int) latestRetentionPeriod.getValueAs(TimeUnit.SECONDS));
		checkOK(connection.sendCommand("insert"));
	}

	/**
	 * Publishes a list of tuples into one or more tables.
	 * 
	 * @param insertStatements
	 *            a list of SQL INSERT statements
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void insert(List<String> insertStatements) throws RGMAPermanentException, RGMATemporaryException {
		try {
			doinsert(insertStatements);
		} catch (UnknownResourceException e) {
			try {
				restore();
				doinsert(insertStatements);
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
	}

	private void doinsert(List<String> insertStatements) throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		ServletConnection connection = getNewConnection();
		connection.setRequestMethodPost();
		for (String insert : insertStatements) {
			connection.addParameter("insert", insert);
		}

		checkOK(connection.sendCommand("insert"));
	}

	/**
	 * Publishes a list of tuples into one or more tables, overriding the latest retention period.
	 * 
	 * @param insertStatements
	 *            a list of SQL INSERT statements
	 * @param latestRetentionPeriod
	 *            latest retention period for these tuples (overrides LRP defined for table)
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public void insert(List<String> insertStatements, TimeInterval latestRetentionPeriod) throws RGMAPermanentException, RGMATemporaryException {
		try {
			doinsert(insertStatements, latestRetentionPeriod);
		} catch (UnknownResourceException e) {
			try {
				restore();
				doinsert(insertStatements, latestRetentionPeriod);
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
	}

	private void doinsert(List<String> insertStatements, TimeInterval latestRetentionPeriod) throws RGMAPermanentException, RGMATemporaryException,
			UnknownResourceException {
		ServletConnection connection = getNewConnection();
		connection.setRequestMethodPost();
		for (Iterator<String> iter = insertStatements.iterator(); iter.hasNext();) {
			String insert = iter.next();
			connection.addParameter("insert", insert);
		}
		connection.addParameter("lrpSec", (int) latestRetentionPeriod.getValueAs(TimeUnit.SECONDS));
		checkOK(connection.sendCommand("insert"));
	}
}
