/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.producer.ondemand;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.SSLSocketFactory;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.producer.ProducerResource;
import org.glite.rgma.server.services.schema.Authz;
import org.glite.rgma.server.services.sql.Constant;
import org.glite.rgma.server.services.sql.DataType;
import org.glite.rgma.server.services.sql.Expression;
import org.glite.rgma.server.services.sql.SQLExpEvaluator;
import org.glite.rgma.server.services.sql.SelectItem;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.TableName;
import org.glite.rgma.server.services.sql.TableNameAndAlias;
import org.glite.rgma.server.services.sql.TableReference;
import org.glite.rgma.server.services.sql.Tuple;
import org.glite.rgma.server.services.sql.WhereClause;
import org.glite.rgma.server.services.sql.DataType.Type;
import org.glite.rgma.server.services.sql.SQLExpEvaluator.NullFound;
import org.glite.rgma.server.services.sql.Tuple.UnknownAttribute;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.ProducerProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.SchemaColumnDefinition;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.Units;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserSystemContext;

/**
 * OnDemandProducer.
 */
public class OnDemandProducerResource extends ProducerResource {

	/**
	 * Thread used to execute a single one-time query on this producer.
	 */
	private class ExecuteThread extends Thread {

		/** Socket connection to user code. */
		private Socket m_userSocket;

		/** Start time of query in milliseconds. */
		private long m_queryEndTimeMS;

		private ODPQuery m_odpQuery;

		private TableNameAndAlias m_tableName;

		private Map<String, DataType.Type> m_cols = new HashMap<String, DataType.Type>();

		private OnDemandProducerTable m_table;

		private Expression m_authPredicateR;

		private Expression m_authPredicateW;

		public ExecuteThread(SelectStatement select, UserSystemContext consumerContext, TimeInterval timeout, OnDemandProducerTable table, ODPQuery odpQuery)
				throws RGMAPermanentException {
			m_table = table;
			List<String> emptyListofVdbTableNames = new ArrayList<String>();
			m_authPredicateR = Authz.constructAuthPredicate(emptyListofVdbTableNames, consumerContext.getDN(), consumerContext.getFQANs(), m_table.m_authz,
					Authz.RuleType.DATA, 'R');
			m_authPredicateW = Authz.constructAuthPredicate(emptyListofVdbTableNames, m_context.getDN(), m_context.getFQANs(), m_table.m_authz,
					Authz.RuleType.DATA, 'W');
			if (timeout != null) {
				m_queryEndTimeMS = System.currentTimeMillis() + timeout.getValueAs(Units.MILLIS);
			} else {
				m_queryEndTimeMS = Long.MAX_VALUE;
			}
			m_odpQuery = odpQuery;
			m_tableName = select.getFrom().get(0).getTable();
			List<SchemaColumnDefinition> cols = m_table.m_def.getColumns();
			for (SelectItem item : select.getSelect()) {
				if (item.isExpression()) {
					String col = ((Constant) item.getExpression()).getValue();
					for (SchemaColumnDefinition scd : cols) {
						if (scd.getName().equalsIgnoreCase(col)) {
							m_cols.put(col, scd.getType().getType());
						}
					}
				}
			}
			SelectStatement s = new SelectStatement();
			s.addSelect(select.getSelect());
			s.addWhere(select.getWhere());
			List<TableReference> ltr = new ArrayList<TableReference>(1);
			TableReference t = new TableReference(m_tableName);
			ltr.add(t);
			s.addFrom(ltr);
			m_userSocket = connectToUserCode(s, s_streamingChunkSize);
		}

		/**
		 * Reads data from user code (using execute) and streams to streaming server.
		 */
		@Override
		public void run() {
			try {
				BufferedReader in = new BufferedReader(new InputStreamReader(m_userSocket.getInputStream()));
				boolean readCompleted = false;
				StringBuilder response = new StringBuilder();
				/* Use the byte read function to avoid blocking */
				char[] fromUserCode = new char[1];
				int endXML = 0;
				do {
					int nbytes = in.read(fromUserCode, 0, 1);
					if (nbytes == -1) {
						readCompleted = true;
					} else if (nbytes == 1) {
						response.append(fromUserCode);
						if (fromUserCode[0] == END_XML.charAt(endXML)) {
							endXML++;
							if (endXML == END_XML.length()) {
								ODPXMLParser parser = new ODPXMLParser(response.toString());
								TupleSet resultSet = parser.getTupleSet();
								OnDemandMetaData md = parser.getMetaData();
								if (m_logger.isDebugEnabled()) {
									m_logger.debug(OnDemandProducerResource.this + " read resultSet from user code with " + resultSet.size() + " tuples.");
								}
								resultSet = filterTuples(resultSet, md);
								if (resultSet.isEndOfResults()) {
									readCompleted = true;
								} else {
									response = new StringBuilder();
									endXML = 0;
								}
								synchronized (m_odpQuery) {
									m_odpQuery.m_resultSet = resultSet;
									s_streamingSender.dataAddedToTupleStore(null);
									if (!readCompleted) {
										while (m_odpQuery.m_resultSet != null) {
											try {
												if (m_logger.isDebugEnabled()) {
													m_logger.debug(OnDemandProducerResource.this + " waiting before reading from user code");
												}
												m_odpQuery.wait();
											} catch (InterruptedException e) {
												interrupt();
												break;
											}
										}
									}
								}
							}
						} else {
							endXML = 0;
						}
					}
				} while (!isInterrupted() && !readCompleted && (System.currentTimeMillis() < m_queryEndTimeMS));
				if (!isInterrupted() && !readCompleted) {
					m_logger.warn(OnDemandProducerResource.this + " Query timed out to user code.");
				} else if (isInterrupted()) {
					m_logger.warn(OnDemandProducerResource.this + " was interrupted.");
				}
			} catch (IOException e) {
				m_logger.warn(OnDemandProducerResource.this + " IOException connecting to user code " + e.getMessage());
			} catch (RGMAPermanentException e) {
				m_logger.warn(OnDemandProducerResource.this + " User code returned " + e.getFlattenedMessage());
			}
			if (m_logger.isDebugEnabled()) {
				m_logger.debug(OnDemandProducerResource.this + " closing down socket.");
			}
			try {
				m_userSocket.close();
			} catch (IOException io) {
				m_logger.warn(OnDemandProducerResource.this + " could not close connection to user code.", io);
			}
		}

		private TupleSet filterTuples(TupleSet resultSet, OnDemandMetaData rsm) throws RGMAPermanentException {
			/* First check metadata is as expected */
			String warningMessage = null;
			try {
				if (rsm.getColumnCount() != m_cols.size()) {
					throw new RGMAPermanentException("Metadata from ODP user code has " + rsm.getColumnCount() + " columns rather than " + m_cols.size());
				}
				for (String tableName : rsm.getTableNames()) {
					if (!tableName.equalsIgnoreCase(m_tableName.getVdbTableName())) {
						throw new RGMAPermanentException("Metadata from ODP user code has table name " + tableName + " rather than "
								+ m_tableName.getVdbTableName());
					}
				}
				for (Entry<String, DataType.Type> e : m_cols.entrySet()) {
					String col = e.getKey();
					DataType.Type t = e.getValue();
					int n = rsm.getColumnNumber(col);
					if (n < 0) {
						throw new RGMAPermanentException("Metadata from ODP user code does not have expected column " + col);
					}
					if (rsm.getColumnType(n).getType() != t) {
						throw new RGMAPermanentException("Metadata from ODP user code has " + col + " of type " + rsm.getColumnType(n).getType()
								+ " rather than " + t);
					}
				}
			} catch (RGMAPermanentException e) {
				TupleSet nrs = new TupleSet();
				nrs.setWarning(e.getMessage());
				if (m_logger.isDebugEnabled()) {
					m_logger.debug(OnDemandProducerResource.this + " rejecting all tuples. RGMAUserException " + e.getMessage());
				}
				return nrs;
			}

			/* Now look at the data */
			List<String[]> goodData = new ArrayList<String[]>();
			for (String[] userData : resultSet.getData()) {
				Tuple tuple = null;
				try {
					tuple = new Tuple();
					for (int i = 0; i < userData.length; i++) {

						String colName = rsm.getColumnName(i + 1);
						Type colType = rsm.getColumnType(i + 1).getType();
						String value = userData[i];
						if (value == null || colType == Type.CHAR || colType == Type.VARCHAR || colType == Type.DATE || colType == Type.TIME
								|| colType == Type.TIMESTAMP) {
							tuple.addAttribute(colName, value);
						} else if (colType == Type.INTEGER) {
							tuple.addAttribute(colName, Integer.parseInt(value));
						} else if (colType == Type.REAL) {
							tuple.addAttribute(colName, Float.parseFloat(value));
						} else if (colType == Type.DOUBLE_PRECISION) {
							tuple.addAttribute(colName, Double.parseDouble(value));
						}
					}

					SQLExpEvaluator exp = new SQLExpEvaluator();

					/* Check producer predicate */
					String predicate = m_table.m_predicate.toString();
					if (predicate.length() != 0) {
						try {
							if (!exp.eval(tuple, WhereClause.parse(predicate).getExpression())) {
								throw new RGMAPermanentException("Tuple is not consistent with producer predicate.");
							}
						} catch (NullFound e) {
							throw new RGMAPermanentException("Predicate may not contain IS NULL nor IS NOT NULL.");
						} catch (UnknownAttribute e) {
							throw new RGMAPermanentException("Tuple is not consistent with producer predicate.");
						}
					}

					/*
					 * Check against read and write authorization rules - the list of table names is not required
					 */
					try {
						if (!exp.eval(tuple, m_authPredicateR)) {
							throw new RGMAPermanentException("Tuple is not compatible with read rules for this table.");
						}
						if (!exp.eval(tuple, m_authPredicateW)) {
							throw new RGMAPermanentException("Tuple is not compatible with write rules for this table.");
						}
					} catch (NullFound e) {
						throw new RGMAPermanentException(e);
					} catch (UnknownAttribute e) {
						throw new RGMAPermanentException("Unknown attribute noted when checking authz rules");
					}

					goodData.add(userData);
					if (m_logger.isDebugEnabled()) {
						m_logger.debug(OnDemandProducerResource.this + " accepting tuple " + tuple);
					}
				} catch (ParseException e) {
					RGMAPermanentException re = new RGMAPermanentException("Parsing error: " + e.getMessage());
					logReject(m_table, re);
					if (warningMessage == null) {
						warningMessage = "Some tuples omitted. " + re.getMessage();
					}
					if (m_logger.isDebugEnabled()) {
						m_logger.debug(OnDemandProducerResource.this + " rejecting " + tuple + " RGMAUserException " + e.getMessage());
					}
				} catch (RGMAPermanentException e) {
					logReject(m_table, e);
					if (warningMessage == null) {
						warningMessage = "Some tuples omitted. " + e.getMessage();
					}
					if (m_logger.isDebugEnabled()) {
						m_logger.debug(OnDemandProducerResource.this + " rejecting " + tuple + " RGMAUserException " + e.getMessage());
					}
				} catch (SQLException e) {
					RGMAPermanentException re = new RGMAPermanentException("SQL Exception " + e.getMessage());
					logReject(m_table, re);
					if (warningMessage == null) {
						warningMessage = "Some tuples omitted. " + re.getMessage();
					}
					if (m_logger.isDebugEnabled()) {
						m_logger.debug(OnDemandProducerResource.this + " rejecting " + tuple + " RGMAUserException " + e.getMessage());
					}
				}
			}
			TupleSet nrs = new TupleSet();
			nrs.addRows(goodData);
			if (warningMessage != null) {
				nrs.setWarning(warningMessage);
			}
			return nrs;
		}
	}

	private class ODPQuery {
		private TupleSet m_resultSet;
		private ExecuteThread m_excuteThread;
	}

	private class OnDemandProducerTable extends Table {
		private Map<ResourceEndpoint, ODPQuery> m_odpQuery = new HashMap<ResourceEndpoint, ODPQuery>();

		private OnDemandProducerTable(Table table) {
			super(table);
		}
	}

	/** Tag indicating the end of an XML block from the user code. */
	private static final String END_XML = "</edg:XMLResponse>";

	private static SSLSocketFactory s_sslSocketFactory;

	public static void setStaticVariables(OnDemandProducerService service, TimeInterval registryTerminationInterval, String hostname,
			SSLSocketFactory sslSocketFactory) throws RGMAPermanentException {
		ProducerResource.setStaticVariables(registryTerminationInterval, hostname);
		s_sslSocketFactory = sslSocketFactory;
	}

	/** Host name for user code. */
	private String m_hostName;

	/** Port for user code. */
	private int m_port;

	private static final ProducerProperties properties = new ProducerProperties();

	public OnDemandProducerResource(UserContext userContext, String hostName, int port, ResourceEndpoint endpoint) throws RGMAPermanentException,
			RGMAPermanentException {
		super(endpoint, userContext, properties, Logger.getLogger(OnDemandProducerConstants.ONDEMAND_PRODUCER_LOGGER));
		m_status = Status.ACTIVE;
		m_port = port;
		m_hostName = hostName;
	}

	@Override
	public boolean canDestroy() throws RGMAPermanentException {
		return true;
	}

	public void closeQuery(String vdbTableName, ResourceEndpoint consumerEp) {
		OnDemandProducerTable table = null;
		synchronized (m_tables) {
			table = (OnDemandProducerTable) m_tables.get(vdbTableName);
		}
		ODPQuery odpQuery = null;
		synchronized (table.m_odpQuery) {
			odpQuery = table.m_odpQuery.remove(consumerEp);
		}
		odpQuery.m_excuteThread.interrupt();
	}

	public void declareTable(UserContext userContext, String userTableName, String predicate) throws RGMAPermanentException, RGMAPermanentException,
			RGMAPermanentException {
		checkContext(userContext);
		TableName ctn = new TableName(userTableName);
		String vdbTableName = ctn.getVdbTableName();
		OnDemandProducerTable t;
		synchronized (m_tables) {
			t = (OnDemandProducerTable) m_tables.get(vdbTableName);
		}
		t = new OnDemandProducerTable(super.declareTable(userContext, ctn, predicate, 0, false));
		synchronized (m_tables) {
			m_tables.put(t.m_vdbTableName, t);
		}
		registerTable(t, 0); // Initial time is zero
	}

	@Override
	public synchronized void destroy() throws RGMAPermanentException {
		synchronized (m_status) {
			if (m_status == Status.DESTROYED) {
				return;
			}
		}
		try {
			super.destroy();
		} finally {
			synchronized (m_status) {
				m_status = Status.DESTROYED;
			}
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Destroyed OnDemand producer resource " + m_endpoint.getResourceID());
		}
	}

	public void execute(SelectStatement select, UserSystemContext consumerContext, String vdbTableName, ResourceEndpoint consumerEp, TimeInterval timeout)
			throws RGMAPermanentException {
		OnDemandProducerTable table = null;
		synchronized (m_tables) {
			table = (OnDemandProducerTable) m_tables.get(vdbTableName);
		}
		ODPQuery odpQuery = new ODPQuery();
		ExecuteThread t = new ExecuteThread(select, consumerContext, timeout, table, odpQuery);
		odpQuery.m_excuteThread = t;
		synchronized (table.m_odpQuery) {
			table.m_odpQuery.put(consumerEp, odpQuery);
		}
		t.start();
	}

	public TupleSet pop(String vdbTableName, ResourceEndpoint consumerEp) {
		OnDemandProducerTable table = null;
		synchronized (m_tables) {
			table = (OnDemandProducerTable) m_tables.get(vdbTableName);
		}
		ODPQuery odpQuery = null;
		synchronized (table.m_odpQuery) {
			odpQuery = table.m_odpQuery.get(consumerEp);
		}
		synchronized (odpQuery) {
			TupleSet rs = odpQuery.m_resultSet;
			if (rs != null) {
				odpQuery.m_resultSet = null;
				odpQuery.notifyAll();
				return rs;
			} else {
				return new TupleSet();
			}
		}
	}

	@Override
	public void updateRegistry() {
		try {
			synchronized (this) {
				synchronized (m_status) {
					if (m_status == Status.DESTROYED) {
						return;
					}
				}
				synchronized (m_tables) {
					for (Table t : m_tables.values()) {
						/* hrpSecs is not used for static producer - so record 0 */
						registerTable(t, 0);
						if (m_logger.isInfoEnabled()) {
							m_logger.info("Updated registry for PrimaryResource ID :" + m_endpoint.getResourceID() + " For Table :" + t.m_vdbTableName);
						}
					}
				}
			}
		} catch (RGMAPermanentException e) {
			m_logger.warn("registerTable failed with internal exception " + e.getFlattenedMessage());
		}
	}

	/**
	 * Connects to the user code and send the SQL SELECT query and number of tuples allowed per ResultSet.
	 * 
	 * @param select
	 *            SQL SELECT query.
	 * @param maxTuplesPerResultSet
	 *            Maximum number of tuples allowed per ResultSet.
	 * @return Socket connected to user code, ready for reading tuples.
	 * @throws RGMAPermanentException
	 *             If an error occurs connecting to the user code.
	 */
	private Socket connectToUserCode(SelectStatement select, int maxTuplesPerResultSet) throws RGMAPermanentException {
		Socket socket;
		try {
			socket = s_sslSocketFactory.createSocket(m_hostName, m_port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), false);
			String queryStr = select + ";" + maxTuplesPerResultSet + "\n";
			out.print(queryStr);
			out.flush();
			if (m_logger.isDebugEnabled()) {
				m_logger.debug("Connected to user code at " + m_hostName + ":" + m_port + " for " + queryStr);
			}
		} catch (IOException e) {
			throw new RGMAPermanentException("Could not connect to user code at " + m_hostName + ":" + m_port, e);
		}

		return socket;
	}

	public String getDetails(boolean tableDetails, boolean queryDetails, boolean fullDetails) {
		StringBuilder b = new StringBuilder();
		b.append("<Resource ID=\"");
		b.append(getEndpoint().getResourceID());
		b.append("\" ClientHostName=\"");
		b.append(getClientHostName());
		b.append("\" UserLastContactIntervalMillis=\"");
		b.append(getLastContactIntervalMillis());
		b.append("\" LastRegistryUpdateIntervalMillis=\"");
		b.append(getLastRegistryUpdateMillis());
		b.append("\" TerminationIntervalMillis=\"");
		b.append(getTerminationInterval() * 1000);
		b.append("\" ResourceCreationTimeMillis=\"");
		b.append(getTimeCreated());
		synchronized (m_tables) {
			b.append("\" TableCount=\"").append(m_tables.size());
		}
		b.append("\" Status=\"").append(displayStatus());
		b.append("\">\n");

		if (tableDetails) {
			synchronized (m_tables) {
				for (Table t : m_tables.values()) {
					synchronized (t) {
						b.append("<Table Name=\"");
						b.append(t.m_vdbTableName);
						b.append("\" HistoryRetentionPeriodMillis=\"");
						b.append(t.m_hrpSecs * 1000);
						b.append("\" LastRegistryUpdateIntervalMillis=\"");
						b.append((t.m_lastRegistryUpdate == 0 ? "Not registered yet" : System.currentTimeMillis() - t.m_lastRegistryUpdate));
						b.append("\" LastSuccessfulInsertIntervalMillis=\"");
						b.append((t.m_lastInsertTime == 0 ? "Awaiting first Insert" : System.currentTimeMillis() - t.m_lastInsertTime));
						b.append("\" TotalNumberInsertedTuples=\"");
						b.append(t.m_totalInsertedTuples);
						b.append("\" TotalNumberRejectedTuples=\"");
						b.append(t.m_totalRejectedTuples);
						b.append("\" LastRejectedTupleIntervalMillis=\"");
						b.append((t.m_lastRejectedTime == 0 ? "None rejected" : System.currentTimeMillis() - t.m_lastRejectedTime));
						b.append("\" LastRejectedTupleException=\"");
						b.append(t.m_lastRejectedExceptionMsg);
						b.append("\" ConsumerCount=\"");
						b.append(t.m_queries.size());
						b.append("\" Predicate=\"");
						b.append(t.m_predicate.toString().length() > 0);
						b.append("\">\n");

						if (queryDetails) {
							synchronized (t.m_queries) {
								for (Entry<ResourceEndpoint, Query> entry : t.m_queries.entrySet()) {
									ResourceEndpoint consumer = entry.getKey();
									Query query = entry.getValue();
									b.append("<RunningQuery ");
									b.append("ConsumerID=\"");
									b.append(consumer.getResourceID());
									b.append("\" ConsumerURL=\"");
									b.append(consumer.getURL());
									b.append("\" ConsumerPort=\"");
									b.append(query.m_streamingPort);
									b.append("\" Predicate=\"");
									b.append(query.m_hasPredicate);
									b.append("\"/>\n");
								}
							}
						}
						b.append("</Table>");
					}
				}
			}
		} else {
			String tableName = "";
			synchronized (m_tables) {
				for (Table table : m_tables.values()) {
					tableName = table.m_vdbTableName;
					break;
				}
			}
			b.append("<Table ");
			b.append("Name=\"" + tableName + "\"");
			b.append(">\n");
			b.append("</Table>");
		}

		if (fullDetails) {
			b.append(getTasksDisplay());
		}
		b.append("</Resource>\n");
		return b.toString();
	}
}
