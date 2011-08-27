/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma;

import java.util.ArrayList;
import java.util.List;

/**
 * Retrieve data from one or more producers by executing a specified SQL query.
 */
public class Consumer extends Resource {

	private String m_query;
	private QueryType m_queryType;
	private TimeInterval m_timeout;
	private QueryTypeWithInterval m_queryTypeWithInterval;
	private List<ResourceEndpoint> m_endpoints;
	private TimeInterval m_queryInterval;
	private boolean m_eof;

	private int create() throws RGMAPermanentException, RGMATemporaryException {
		m_servletName = "ConsumerServlet";
		ServletConnection consumerServlet = new ServletConnection(m_servletName);
		consumerServlet.addParameter("select", m_query);
		if (m_queryType == QueryType.C || m_queryTypeWithInterval == QueryTypeWithInterval.C) {
			consumerServlet.addParameter("queryType", "continuous");
		} else if (m_queryType == QueryType.H || m_queryTypeWithInterval == QueryTypeWithInterval.H) {
			consumerServlet.addParameter("queryType", "history");
		} else if (m_queryType == QueryType.L || m_queryTypeWithInterval == QueryTypeWithInterval.L) {
			consumerServlet.addParameter("queryType", "latest");
		} else if (m_queryType == QueryType.S) {
			consumerServlet.addParameter("queryType", "static");
		}
		if (m_queryInterval != null) {
			consumerServlet.addParameter("timeIntervalSec", m_queryInterval.getValueAs(TimeUnit.SECONDS));
		}
		if (m_timeout != null) {
			consumerServlet.addParameter("timeoutSec", m_timeout.getValueAs(TimeUnit.SECONDS));
		}
		if (m_endpoints != null) {
			for (ResourceEndpoint endpoint : m_endpoints) {
				String endpointString = endpoint.getResourceId() + " " + endpoint.getUrlString();
				consumerServlet.addParameter("producerConnections", endpointString);
			}
		}
		Tuple tuple = consumerServlet.sendNonResourceCommand("createConsumer").getData().get(0);
		return tuple.getInt(0);
	}

	private Consumer(String query, QueryType queryType, TimeInterval timeout, QueryTypeWithInterval queryTypeWithInterval, List<ResourceEndpoint> endpoints,
			TimeInterval queryInterval) {
		m_query = query;
		m_queryType = queryType;
		m_timeout = timeout;
		m_queryTypeWithInterval = queryTypeWithInterval;
		m_endpoints = endpoints;
		m_queryInterval = queryInterval;
	}

	/**
	 * Creates a consumer with the specified query type and where a timeout and list of producers may also be specified.
	 * For a continuous query, only tuples published after the consumer has been created will be returned. All tuples
	 * are included for other query types. The query will terminate after the specified timeout and will make use of the
	 * specified list of producers.
	 * 
	 * @param query
	 *            a SQL select statement
	 * @param queryType
	 *            the type of the query
	 * @param timeout
	 *            time interval after which the query will be aborted.
	 * @param producers
	 *            list of producers to contact.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Consumer(String query, QueryType queryType, TimeInterval timeout, List<ResourceEndpoint> producers) throws RGMAPermanentException,
			RGMATemporaryException {
		m_query = query;
		m_queryType = queryType;
		m_timeout = timeout;
		m_endpoints = new ArrayList<ResourceEndpoint>(producers);
		m_resourceId = create();
	}

	/**
	 * Creates a consumer with the specified query type and where a timeout may also be specified. For a continuous
	 * query, only tuples published after the consumer has been created will be returned. All tuples are included for
	 * other query types. The query will terminate after the specified timeout and the mediator will be used to find
	 * suitable producers.
	 * 
	 * @param query
	 *            a SQL select statement
	 * @param queryType
	 *            the type of the query
	 * @param timeout
	 *            time interval after which the query will be aborted. May be null to have no timeout.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Consumer(String query, QueryType queryType, TimeInterval timeout) throws RGMAPermanentException, RGMATemporaryException {
		m_query = query;
		m_queryType = queryType;
		m_timeout = timeout;
		m_resourceId = create();
	}

	/**
	 * Creates a consumer with the specified query type and where a list of producers may also be specified. For a
	 * continuous query, only tuples published after the consumer has been created will be returned. All tuples are
	 * included for other query types. The query will make use of the specified list of producers.
	 * 
	 * @param query
	 *            a SQL select statement
	 * @param queryType
	 *            the type of the query
	 * @param producers
	 *            list of producers to contact.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Consumer(String query, QueryType queryType, List<ResourceEndpoint> producers) throws RGMAPermanentException, RGMATemporaryException {
		m_query = query;
		m_queryType = queryType;
		m_endpoints = new ArrayList<ResourceEndpoint>(producers);
		m_resourceId = create();
	}

	/**
	 * Creates a consumer with the specified query type. For a continuous query, only tuples published after the
	 * consumer has been created will be returned. All tuples are included for other query types. The mediator will be
	 * used to find suitable producers.
	 * 
	 * @param query
	 *            a SQL select statement
	 * @param queryType
	 *            the type of the query
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Consumer(String query, QueryType queryType) throws RGMAPermanentException, RGMATemporaryException {
		m_query = query;
		m_queryType = queryType;
		m_resourceId = create();
	}

	/**
	 * Creates a consumer with the specified query type and query interval and where a timeout and list of producers may
	 * also be specified. The query will terminate after the specified timeout and will make use of the specified list
	 * of producers.
	 * 
	 * @param query
	 *            a SQL select statement
	 * @param queryType
	 *            the type of the query
	 * @param queryInterval
	 *            the time interval is subtracted from the current time to give a time in the past. The query result
	 *            will then use data from tuples published after this time.
	 * @param timeout
	 *            time interval after which the query will be aborted.
	 * @param producers
	 *            list of producers to contact.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Consumer(String query, QueryTypeWithInterval queryType, TimeInterval queryInterval, TimeInterval timeout, List<ResourceEndpoint> producers)
			throws RGMAPermanentException, RGMATemporaryException {
		m_query = query;
		m_queryTypeWithInterval = queryType;
		m_queryInterval = queryInterval;
		m_timeout = timeout;
		m_endpoints = new ArrayList<ResourceEndpoint>(producers);
		m_resourceId = create();
	}

	/**
	 * Creates a consumer with the specified query type and query interval and where a timeout may also be specified.
	 * The query will terminate after the specified timeout and the mediator will be used to find suitable producers.
	 * 
	 * @param query
	 *            a SQL select statement
	 * @param queryType
	 *            the type of the query
	 * @param queryInterval
	 *            the time interval is subtracted from the current time to give a time in the past. The query result
	 *            will then use data from tuples published after this time.
	 * @param timeout
	 *            time interval after which the query will be aborted.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Consumer(String query, QueryTypeWithInterval queryType, TimeInterval queryInterval, TimeInterval timeout) throws RGMAPermanentException,
			RGMATemporaryException {
		m_query = query;
		m_queryTypeWithInterval = queryType;
		m_queryInterval = queryInterval;
		m_timeout = timeout;
		m_resourceId = create();
	}

	/**
	 * Creates a consumer with the specified query type and query interval and where a list of producers may also be
	 * specified. The query will make use of the specified list of producers.
	 * 
	 * @param query
	 *            a SQL select statement
	 * @param queryType
	 *            the type of the query
	 * @param queryInterval
	 *            the time interval is subtracted from the current time to give a time in the past. The query result
	 *            will then use data from tuples published after this time.
	 * @param producers
	 *            list of producers to contact.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Consumer(String query, QueryTypeWithInterval queryType, TimeInterval queryInterval, List<ResourceEndpoint> producers) throws RGMAPermanentException,
			RGMATemporaryException {
		m_query = query;
		m_queryTypeWithInterval = queryType;
		m_queryInterval = queryInterval;
		m_endpoints = new ArrayList<ResourceEndpoint>(producers);
		m_resourceId = create();
	}

	/**
	 * Creates a consumer with the specified query type and query interval. The mediator will be used to find suitable
	 * producers.
	 * 
	 * @param query
	 *            a SQL select statement
	 * @param queryType
	 *            the type of the query
	 * @param queryInterval
	 *            the time interval is subtracted from the current time to give a time in the past. The query result
	 *            will then use data from tuples published after this time.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public Consumer(String query, QueryTypeWithInterval queryType, TimeInterval queryInterval) throws RGMAPermanentException, RGMATemporaryException {
		m_query = query;
		m_queryTypeWithInterval = queryType;
		m_queryInterval = queryInterval;
		m_resourceId = create();
	}

	/**
	 * Aborts the query. Pop can still be called to retrieve existing data.
	 * 
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 * @see #hasAborted
	 */
	public void abort() throws RGMAPermanentException, RGMATemporaryException {
		try {
			doAbort();
		} catch (UnknownResourceException e) {
			try {
				restore();
				doAbort();
				return;
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
	}

	private void doAbort() throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		ServletConnection connection = getNewConnection();
		checkOK(connection.sendCommand("abort"));
	}

	/**
	 * Determines if the query has aborted.
	 * 
	 * @return <code>true</code> if the query was aborted either by a user call or a timeout event
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 * @see #abort
	 */
	public boolean hasAborted() throws RGMAPermanentException, RGMATemporaryException {
		try {
			return doHasAborted();
		} catch (UnknownResourceException e) {
			try {
				restore();
				return doHasAborted();
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
	}

	private boolean doHasAborted() throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		ServletConnection connection = getNewConnection();
		Tuple tuple = connection.sendCommand("hasAborted").getData().get(0);
		return tuple.getBoolean(0);
	}

	/**
	 * Retrieves tuples from the result of the query.
	 * 
	 * @param maxCount
	 *            the maximum number of tuples to retrieve
	 * @return a tuple set containing the received tuples. A set with an empty list of tuples is returned if none were
	 *         found.
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public TupleSet pop(int maxCount) throws RGMAPermanentException, RGMATemporaryException {
		try {
			return doPop(maxCount);
		} catch (UnknownResourceException e) {
			try {
				restore();
				TupleSet ts = doPop(maxCount);
				ts.appendWarning("The query was restarted - many duplicates may be returned.");
				return ts;
			} catch (UnknownResourceException e1) {
				throw new RGMATemporaryException(e1.getMessage());
			}
		}
	}

	private TupleSet doPop(int maxCount) throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		ServletConnection connection = getNewConnection();
		connection.addParameter("maxCount", maxCount);
		TupleSet ts = connection.sendCommand("pop");
		if (m_eof) {
			ts.appendWarning("You have called pop again after end of results returned.");
		}
		m_eof = ts.isEndOfResults();
		return ts;
	}

	private void restore() throws RGMAPermanentException, RGMATemporaryException {
		Consumer c = new Consumer(m_query, m_queryType, m_timeout, m_queryTypeWithInterval, m_endpoints, m_queryInterval);
		m_resourceId = c.create();
	}

}
