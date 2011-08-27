/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.servlets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.servlet.GenericServlet;
import javax.servlet.ServletException;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.glite.rgma.server.services.ClientAccessConfigurator;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.VDBConfigurator;
import org.glite.rgma.server.services.consumer.ConsumerService;
import org.glite.rgma.server.services.producer.ProducerService;
import org.glite.rgma.server.services.producer.ondemand.OnDemandProducerService;
import org.glite.rgma.server.services.producer.primary.PrimaryProducerService;
import org.glite.rgma.server.services.producer.secondary.SecondaryProducerService;
import org.glite.rgma.server.services.producer.store.TupleStoreManager;
import org.glite.rgma.server.services.registry.RegistryService;
import org.glite.rgma.server.services.resource.ResourceManagementService;
import org.glite.rgma.server.services.schema.SchemaService;
import org.glite.rgma.server.services.streaming.StreamingReceiver;
import org.glite.rgma.server.services.streaming.StreamingSender;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.system.ConsumerEntry;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.ProducerProperties;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.SchemaColumnDefinition;
import org.glite.rgma.server.system.SchemaIndex;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.Storage;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.Units;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserSystemContext;
import org.glite.voms.FQAN;
import org.glite.voms.VOMSValidator;

/**
 * Class providing methods common to all servlets.
 */
@SuppressWarnings("serial")
public class RGMAServlet extends HttpServlet {

	private class ConsumerServiceMapper {
		private ConsumerService m_service = null;

		ConsumerServiceMapper() throws UnavailableException {
			try {
				m_service = ConsumerService.getInstance();
			} catch (RGMAPermanentException e) {
				throw new UnavailableException(e.getFlattenedMessage());
			}
		}

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer)

		throws RGMATemporaryException, RGMAPermanentException, UnknownResourceException, IOException {

			if (operation.equals(ServletConstants.M_CREATE_CONSUMER)) {

				String queryType = getStringParameter(ServletConstants.P_QUERY_TYPE, request);
				boolean isContinuous = queryType.equals("continuous");
				boolean isHistory = queryType.equals("history");
				boolean isStatic = queryType.equals("static");
				boolean isLatest = queryType.equals("latest");
				String select = getStringParameter(ServletConstants.P_SELECT, request);

				QueryProperties queryProps = null;

				int timeIntervalSec = -1;
				if (request.getParameter(ServletConstants.P_TIME_INTERVAL_SEC) != null) {
					timeIntervalSec = getIntParameter(ServletConstants.P_TIME_INTERVAL_SEC, request);
				}

				if (isContinuous) {
					if (timeIntervalSec != -1) {
						queryProps = QueryProperties.getContinuous(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProps = QueryProperties.CONTINUOUS;
					}
				} else if (isHistory) {
					if (timeIntervalSec != -1) {
						queryProps = QueryProperties.getHistory(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProps = QueryProperties.HISTORY;
					}
				} else if (isLatest) {
					if (timeIntervalSec != -1) {
						queryProps = QueryProperties.getLatest(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProps = QueryProperties.LATEST;
					}
				} else if (isStatic) {
					queryProps = QueryProperties.STATIC;
				} else {
					throw new RGMAPermanentException("No continuous, latest, history or static flag found.");
				}

				TimeInterval timeout = null;
				if (request.getParameter(ServletConstants.P_TIMEOUT) != null) {
					timeout = new TimeInterval(getIntParameter(ServletConstants.P_TIMEOUT, request), Units.SECONDS);
				}

				String[] producerConnections = request.getParameterValues(ServletConstants.P_DIRECTED_PRODUCER);
				List<ResourceEndpoint> producers = null;
				if ((producerConnections != null) && (producerConnections.length > 0)) {
					producers = new ArrayList<ResourceEndpoint>();
					for (String element : producerConnections) {
						ResourceEndpoint endpoint = decodeResourceEndpoint(element);
						producers.add(endpoint);
					}
				}
				int resourceId = m_service.createConsumer(getUserContext(request), select, queryProps, timeout, producers);
				writer.writeInt(resourceId);

			} else if (operation.equals(ServletConstants.M_ADD_PRODUCER)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String url = getStringParameter(ServletConstants.P_URL, request);
				int id = getIntParameter(ServletConstants.P_ID, request);
				String vdbName = getStringParameter(ServletConstants.P_VDB_NAME, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				String predicate = getStringParameter(ServletConstants.P_PREDICATE, request);
				int hrpSec = (int) getLongParameter(ServletConstants.P_HRP_SEC, request);
				ResourceEndpoint re = null;
				try {
					re = new ResourceEndpoint(new URL(url), id);
				} catch (MalformedURLException e) {
					throw new RGMAPermanentException(e);
				}
				ProducerType pt = new ProducerType(getBooleanParameter(ServletConstants.P_IS_HISTORY, request), getBooleanParameter(
						ServletConstants.P_IS_LATEST, request), getBooleanParameter(ServletConstants.P_IS_CONTINUOUS, request), getBooleanParameter(
						ServletConstants.P_IS_STATIC, request), getBooleanParameter(ServletConstants.P_IS_SECONDARY, request));
				ProducerTableEntry producerTable = new ProducerTableEntry(re, vdbName, tableName, pt, hrpSec, predicate);
				m_service.addProducer(resourceId, producerTable);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_POP)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				int maxCount = getIntParameter(ServletConstants.P_MAX_COUNT, request);
				TupleSet rs = m_service.pop(getUserContext(request), resourceId, maxCount);
				writer.writeResultSet(rs);

			} else if (operation.equals(ServletConstants.M_ABORT)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				m_service.abort(resourceId);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_HAS_ABORTED)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				boolean hasAborted = m_service.hasAborted(resourceId);
				writer.writeBoolean(hasAborted);

			} else if (operation.equals(ServletConstants.M_REMOVE_PRODUCER)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String url = getStringParameter(ServletConstants.P_URL, request);
				int id = getIntParameter(ServletConstants.P_ID, request);
				ResourceEndpoint producer = null;
				try {
					producer = new ResourceEndpoint(new URL(url), id);
				} catch (MalformedURLException e) {
					throw new RGMAPermanentException(e);
				}
				m_service.removeProducer(resourceId, producer);
				writer.writeStatusOK();

			} else {
				m_resourceManagementServiceMapper.doOperation(operation, request, writer, m_service);
			}
		}
	}

	private class OnDemandProducerServiceMapper {

		private OnDemandProducerService m_service = null;

		/**
		 * Creates a new OnDemandProducerServiceMapper object.
		 */
		OnDemandProducerServiceMapper() throws UnavailableException {
			try {
				m_service = OnDemandProducerService.getInstance();
			} catch (RGMAPermanentException e) {
				throw new UnavailableException(e.getFlattenedMessage());
			}
		}

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer) throws RGMATemporaryException, RGMAPermanentException,
				UnknownResourceException, IOException, NumericException {

			if (operation.equals(ServletConstants.M_CREATE_ONDEMANDPRODUCER)) {
				String hostName = getStringParameter("hostName", request);
				int port = getIntParameter("port", request);
				int resourceID = m_service.createOnDemandProducer(getUserContext(request), hostName, port);
				writer.writeInt(resourceID);

			} else if (operation.equals(ServletConstants.M_DECLARE_TABLE)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				String predicate = getStringParameter(ServletConstants.P_PREDICATE, request, "");
				m_service.declareTable(getUserContext(request), resourceId, tableName, predicate);
				writer.writeStatusOK();

			} else {
				m_producerServiceMapper.doOperation(operation, request, writer, m_service);
			}
		}
	}

	private class PrimaryProducerServiceMapper {
		private PrimaryProducerService m_pservice = null;

		/**
		 * Creates a new PrimaryProducerServiceMapper object.
		 */
		PrimaryProducerServiceMapper() throws UnavailableException {
			try {
				m_pservice = PrimaryProducerService.getInstance();
			} catch (RGMAPermanentException e) {
				throw new UnavailableException(e.getFlattenedMessage());
			}
		}

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer) throws RGMATemporaryException, RGMAPermanentException,
				UnknownResourceException, IOException, NumericException {

			if (operation.equals(ServletConstants.M_GET_LRP)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				int lrp = m_pservice.getLatestRetentionPeriod(resourceId, tableName);
				writer.writeInt(lrp);

			} else if (operation.equals(ServletConstants.M_CREATE_PRIMARYPRODUCER)) {
				boolean isHistory = getBooleanParameter(ServletConstants.P_IS_HISTORY, request);
				boolean isLatest = getBooleanParameter(ServletConstants.P_IS_LATEST, request);
				String logicalName = request.getParameter(ServletConstants.P_LOGICAL_NAME);
				String producerType = getStringParameter(ServletConstants.P_TYPE, request);
				ProducerProperties pProps = null;
				if (producerType.equalsIgnoreCase("MEMORY")) {
					pProps = new ProducerProperties(Storage.MEMORY, isHistory, isLatest);
				} else if (producerType.equalsIgnoreCase("DATABASE")) {
					Storage st = null;
					if (logicalName == null) {
						st = Storage.DATABASE;
					} else {
						st = Storage.getDatabase(logicalName);
					}
					pProps = new ProducerProperties(st, isHistory, isLatest);
				} else {
					throw new RGMAPermanentException("Unsupported Producer Type: " + producerType + " on the " + operation);
				}
				int resourceID = m_pservice.createPrimaryProducer(getUserContext(request), pProps);
				writer.writeInt(resourceID);

			} else if (operation.equals(ServletConstants.M_DECLARE_TABLE)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request, null);
				String predicate = getStringParameter(ServletConstants.P_PREDICATE, request, "");
				long hrpSec = getLongParameter(ServletConstants.P_HRP_SEC, request, 0);
				long lrpSec = getLongParameter(ServletConstants.P_LRP, request, 0);
				m_pservice.declareTable(getUserContext(request), resourceId, tableName, predicate, (int) hrpSec, (int) lrpSec);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_INSERT)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String[] insertStrings = request.getParameterValues(ServletConstants.P_INSERT);
				List<String> insertList = new ArrayList<String>();
				for (String insertStatement : insertStrings) {
					insertList.add(insertStatement);
				}
				if (request.getParameter(ServletConstants.P_LRP) == null) {
					m_pservice.insertList(resourceId, insertList, getUserContext(request));
				} else {
					int lrpSec = getIntParameter(ServletConstants.P_LRP, request);
					m_pservice.insertList(resourceId, insertList, lrpSec, getUserContext(request));
				}
				writer.writeStatusOK();

			} else {
				m_producerServiceMapper.doOperation(operation, request, writer, m_pservice);
			}
		}
	}

	private class ProducerServiceMapper {

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer, ProducerService service) throws RGMATemporaryException,
				RGMAPermanentException, RGMAPermanentException, UnknownResourceException, IOException, NumericException {

			if (operation.equals(ServletConstants.M_START)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String select = getStringParameter(ServletConstants.P_SELECT, request);
				QueryProperties queryProps = null;
				String queryType = getStringParameter(ServletConstants.P_QUERY_TYPE, request);
				int timeIntervalSec = -1;
				if (request.getParameter(ServletConstants.P_TIME_INTERVAL_SEC) != null) {
					timeIntervalSec = getIntParameter(ServletConstants.P_TIME_INTERVAL_SEC, request);
				}
				if (queryType.equals("continuous")) {
					if (timeIntervalSec != -1) {
						queryProps = QueryProperties.getContinuous(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProps = QueryProperties.CONTINUOUS;
					}
				} else if (queryType.equals("history")) {
					if (timeIntervalSec != -1) {
						queryProps = QueryProperties.getHistory(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProps = QueryProperties.HISTORY;
					}
				} else if (queryType.equals("latest")) {
					if (timeIntervalSec != -1) {
						queryProps = QueryProperties.getLatest(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProps = QueryProperties.LATEST;
					}
				} else if (queryType.equals("static")) {
					queryProps = QueryProperties.STATIC;
				} else {
					throw new RGMAPermanentException("queryType must be one of 'continuous', 'latest', 'history' or 'static'. Parameter provided = '"
							+ queryType + "'");
				}
				TimeInterval timeout = null;
				if (request.getParameter(ServletConstants.P_TIMEOUT) != null) {
					timeout = new TimeInterval(getIntParameter(ServletConstants.P_TIMEOUT, request), Units.SECONDS);
				}
				String url = getStringParameter(ServletConstants.P_CONSUMER_URL, request);
				int id = getIntParameter(ServletConstants.P_CONSUMER_ID, request);
				ResourceEndpoint consumer;
				try {
					consumer = new ResourceEndpoint(new URL(url), id);
				} catch (MalformedURLException e) {
					throw new RGMAPermanentException(e);
				}
				String streamingURL = getStringParameter(ServletConstants.P_STREAMING_URL, request);
				int streamingPort = getIntParameter(ServletConstants.P_STREAMING_PORT, request);
				int chunkSize = getIntParameter(ServletConstants.P_STREAMING_CHUNK_SIZE, request);
				int streamingProtocol = getIntParameter(ServletConstants.P_STREAMING_PROTOCOL, request);
				StreamingProperties streamingProps = new StreamingProperties(streamingURL, streamingPort, chunkSize, streamingProtocol);
				TupleSet rs = service.start(resourceId, select, queryProps, timeout, consumer, streamingProps, getUserSystemContext(request));
				writer.writeResultSet(rs);

			} else if (operation.equals(ServletConstants.M_ABORT)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String url = getStringParameter(ServletConstants.P_CONSUMER_URL, request);
				int id = getIntParameter(ServletConstants.P_CONSUMER_ID, request);
				ResourceEndpoint consumer;
				try {
					consumer = new ResourceEndpoint(new URL(url), id);
				} catch (MalformedURLException e) {
					throw new RGMAPermanentException(e);
				}
				/* TODO should pass in a context of some sort to allow checking */
				service.abort(resourceId, consumer);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_GET_HRP)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				int hrp = service.getHistoryRetentionPeriod(resourceId, tableName);
				writer.writeInt(hrp);

			} else if (operation.equals(ServletConstants.M_DROP_TUPLE_STORE)) {
				String logicalName = getStringParameter(ServletConstants.P_LOGICAL_NAME, request);
				ProducerService.dropTupleStore(getUserContext(request), logicalName);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_LIST_TUPLE_STORES)) {
				TupleSet rs = ProducerService.listTupleStores(getUserContext(request));
				writer.writeResultSet(rs);

			} else {
				m_resourceManagementServiceMapper.doOperation(operation, request, writer, service);
			}
		}
	}

	private class RegistryServiceMapper {
		private RegistryService m_service = null;

		RegistryServiceMapper() throws UnavailableException {
			try {
				m_service = RegistryService.getInstance();
			} catch (RGMAPermanentException e) {
				throw new UnavailableException(e.getFlattenedMessage());
			}
		}

		private TupleSet convertConsumerEntryLToToResultSet(List<ConsumerEntry> cel) throws RGMAPermanentException {
			TupleSet rs = new TupleSet();
			for (int i = 0; i < cel.size(); i++) {
				ConsumerEntry ce = cel.get(i);
				ResourceEndpoint endpoint = ce.getEndpoint();
				String[] row = { endpoint.getURL().toString(), "" + endpoint.getResourceID() };
				rs.addRow(row);
			}
			rs.setEndOfResults(true);
			return rs;
		}

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer) throws RGMATemporaryException, RGMAPermanentException,
				UnknownResourceException, IOException {

			if (operation.equals(ServletConstants.M_PING)) {
				String vdbName = getStringParameter(ServletConstants.P_VDB_NAME, request);
				m_service.ping(vdbName);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_GET_ALL_PRODUCERS_FOR_TABLE)) {
				String vdbName = getStringParameter(ServletConstants.P_VDB_NAME, request);
				Boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				List<ProducerTableEntry> pdl = m_service.getAllProducersForTable(tableName, canForward, vdbName);
				TupleSet rs = new TupleSet();
				for (ProducerTableEntry pd : pdl) {
					ResourceEndpoint endpoint = pd.getEndpoint();
					String[] row = { endpoint.getURL().toString(), "" + endpoint.getResourceID(), "" + pd.getProducerType().isSecondary(),
							"" + pd.getProducerType().isContinuous(), "" + pd.getProducerType().isStatic(), "" + pd.getProducerType().isHistory(),
							"" + pd.getProducerType().isLatest(), pd.getPredicate(), "" + pd.getHistoryRetentionPeriod()};
					rs.addRow(row);
				}
				rs.setEndOfResults(true);
				writer.writeResultSet(rs);

			} else if (operation.equals(ServletConstants.M_GET_MATCHING_PRODUCERS_FOR_TABLES)) {
				String vdbName = getStringParameter(ServletConstants.P_VDB_NAME, request);
				Boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String[] tables = request.getParameterValues(ServletConstants.P_TABLES);
				List<String> tableNames = null;
				if (tables == null) {
					tableNames = new ArrayList<String>();
				} else {
					tableNames = Arrays.asList(tables);
				}
				String predicate = getStringParameter(ServletConstants.P_PREDICATE, request);
				String type = getStringParameter(ServletConstants.P_QUERY_TYPE, request);
				boolean hasTimeInterval = request.getParameter(ServletConstants.P_TIME_INTERVAL_SEC) != null;
				int timeIntervalSec = 0;
				if (hasTimeInterval) {
					timeIntervalSec = getIntParameter(ServletConstants.P_TIME_INTERVAL_SEC, request);
				}
				boolean isSecondary = false;
				QueryProperties queryProperties = null;
				if (type.equals(ServerConstants.CONTINUOUS)) {
					if (request.getParameter(ServletConstants.P_IS_SECONDARY) != null) {
						isSecondary = getBooleanParameter(ServletConstants.P_IS_SECONDARY, request);
					}
					if (hasTimeInterval) {
						queryProperties = QueryProperties.getContinuous(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProperties = QueryProperties.CONTINUOUS;
					}
				} else if (type.equals(ServerConstants.LATEST)) {
					if (hasTimeInterval) {
						queryProperties = QueryProperties.getLatest(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProperties = QueryProperties.LATEST;
					}
				} else if (type.equals(ServerConstants.HISTORY)) {
					if (hasTimeInterval) {
						queryProperties = QueryProperties.getHistory(new TimeInterval(timeIntervalSec, Units.SECONDS));
					} else {
						queryProperties = QueryProperties.HISTORY;
					}
				} else if (type.equals(ServerConstants.STATIC)) {
					queryProperties = QueryProperties.STATIC;
				}

				ResourceEndpoint consumerEndpoint = null;
				int terminationIntervalSecs = 0;
				if (request.getParameter(ServletConstants.P_RESOURCE_ID) != null) {
					URL consumerurl = null;
					int resourceId = getIntParameter(ServletConstants.P_RESOURCE_ID, request);
					String url = getStringParameter(ServletConstants.P_URL, request);
					try {
						consumerurl = new URL(url);
					} catch (MalformedURLException e) {
						throw new RGMAPermanentException(e);
					}
					consumerEndpoint = new ResourceEndpoint(consumerurl, resourceId);
					terminationIntervalSecs = getIntParameter(ServletConstants.P_TERMINATION_INTERVAL_SEC, request);
				}

				Service.checkSystemContext(vdbName, getSystemContext(request));
				List<ProducerTableEntry> pdl = m_service.getMatchingProducersForTables(vdbName, canForward, tableNames, predicate, queryProperties,
						isSecondary, consumerEndpoint, terminationIntervalSecs);
				TupleSet rs = new TupleSet();
				for (ProducerTableEntry pd : pdl) {
					ResourceEndpoint endpoint = pd.getEndpoint();
					String[] row = { endpoint.getURL().toString(), "" + endpoint.getResourceID(), "" + pd.getProducerType().isSecondary(),
							"" + pd.getProducerType().isContinuous(), "" + pd.getProducerType().isStatic(), "" + pd.getProducerType().isHistory(),
							"" + pd.getProducerType().isLatest(), pd.getPredicate(), "" + pd.getHistoryRetentionPeriod(), pd.getVdbName(), pd.getTableName() };
					rs.addRow(row);
				}
				rs.setEndOfResults(true);
				writer.writeResultSet(rs);

			} else if (operation.equals(ServletConstants.M_REGISTER_PRODUCER_TABLE)) {
				String vdbName = request.getParameter(ServletConstants.P_VDB_NAME);
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				String predicate = request.getParameter(ServletConstants.P_PREDICATE);
				int resourceId = getIntParameter(ServletConstants.P_ID, request);
				String url = getStringParameter(ServletConstants.P_URL, request);
				URL producerurl = null;
				try {
					producerurl = new URL(url);
				} catch (MalformedURLException e) {
					throw new RGMAPermanentException(e);
				}
				ResourceEndpoint producer = new ResourceEndpoint(producerurl, resourceId);
				boolean isHistory = false;
				boolean isLatest = false;
				boolean isStatic = false;
				boolean isContinuous = false;
				boolean isSecondaryProducer = false;
				isHistory = getBooleanParameter(ServletConstants.P_IS_HISTORY, request);
				isLatest = getBooleanParameter(ServletConstants.P_IS_LATEST, request);
				isStatic = getBooleanParameter(ServletConstants.P_IS_STATIC, request);
				isContinuous = getBooleanParameter(ServletConstants.P_IS_CONTINUOUS, request);
				isSecondaryProducer = getBooleanParameter(ServletConstants.P_IS_SECONDARY, request);
				ProducerType producerType = new ProducerType(isHistory, isLatest, isContinuous, isStatic, isSecondaryProducer);
				int hrpSec = getIntParameter(ServletConstants.P_HRP_SEC, request);
				int terminationIntervalSec = getIntParameter(ServletConstants.P_TERMINATION_INTERVAL_SEC, request);
				List<ConsumerEntry> consumers = new LinkedList<ConsumerEntry>();
				Service.checkSystemContext(vdbName, getSystemContext(request));
				consumers = m_service.registerProducerTable(vdbName, canForward, producer, tableName, predicate, producerType, hrpSec, terminationIntervalSec);
				TupleSet rs = convertConsumerEntryLToToResultSet(consumers);
				writer.writeResultSet(rs);

			} else if (operation.equals(ServletConstants.M_UNREGISTER_CONTINUOUS_CONSUMER)) {
				String vdb = getStringParameter(ServletConstants.P_VDB_NAME, request);
				Boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String url = getStringParameter(ServletConstants.P_URL, request);
				URL consumerurl = null;
				try {
					consumerurl = new URL(url);
				} catch (MalformedURLException e) {
					throw new RGMAPermanentException(e);
				}
				int resourceId = getIntParameter(ServletConstants.P_ID, request);
				ResourceEndpoint consumer = new ResourceEndpoint(consumerurl, resourceId);
				Service.checkSystemContext(vdb, getSystemContext(request));
				m_service.unregisterContinuousConsumer(vdb, canForward, consumer);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_UNREGISTER_PRODUCER_TABLE)) {
				String vdbName = request.getParameter(ServletConstants.P_VDB_NAME);
				Boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				String url = getStringParameter(ServletConstants.P_URL, request);
				URL producerurl = null;
				try {
					producerurl = new URL(url);
				} catch (MalformedURLException e) {
					throw new RGMAPermanentException(e);
				}
				int resourceId = getIntParameter(ServletConstants.P_ID, request);
				ResourceEndpoint producer = new ResourceEndpoint(producerurl, resourceId);
				Service.checkSystemContext(vdbName, getSystemContext(request));
				m_service.unregisterProducerTable(vdbName, canForward, tableName, producer);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_ADD_REPLICA)) {
				if (m_service.addReplica(getStringParameter(ServletConstants.P_REPLICA, request))) {
					writer.writeStatusOK();
				} else {
					writer.writeString("Not OK");
				}

			} else {
				m_serviceMapper.doOperation(operation, request, writer, m_service);
			}
		}
	}

	private class ResourceManagementServiceMapper {

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer, ResourceManagementService service)
				throws RGMATemporaryException, RGMAPermanentException, UnknownResourceException, IOException {

			if (operation.equals(ServletConstants.M_PING)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				service.ping(resourceId);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_CLOSE)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				service.close(resourceId);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_DESTROY)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				service.destroy(resourceId);
				writer.writeStatusOK();

			} else {
				m_serviceMapper.doOperation(operation, request, writer, service);
			}
		}
	}

	private class SchemaServiceMapper {
		private SchemaService m_service = null;

		SchemaServiceMapper() throws UnavailableException {
			try {
				m_service = SchemaService.getInstance();
			} catch (RGMAPermanentException e) {
				throw new UnavailableException(e.getFlattenedMessage());
			}
		}

		private TupleSet convertIndexListToResultSet(List<SchemaIndex> indexes) throws RGMAPermanentException {
			TupleSet result = new TupleSet();
			for (SchemaIndex index : indexes) {
				for (String colName : index.getColumnNames()) {
					result.addRow(new String[] { index.getName(), colName });
				}
			}
			result.setEndOfResults(true);
			return result;
		}

		/**
		 * Converts a list of table names into a result set. The result set contains two column headers (tableId, name)
		 * where the tableId is set to -1 for each table name.
		 * 
		 * @throws RGMAPermanentException
		 */
		private TupleSet convertTableNamesToResultSet(List<String> tableNames) throws RGMAPermanentException {
			TupleSet result = new TupleSet();
			for (int i = 0; i < tableNames.size(); i++) {
				result.addRow(new String[] { tableNames.get(i) });
			}
			result.setEndOfResults(true);
			return result;
		}

		/**
		 * @param tableAuthz
		 * @return
		 * @throws RGMAPermanentException
		 */
		private TupleSet convertToResultSet(List<String> tableAuthz) throws RGMAPermanentException {
			TupleSet result = new TupleSet();
			for (int i = 0; i < tableAuthz.size(); i++) {
				result.addRow(new String[] { tableAuthz.get(i) });
			}
			result.setEndOfResults(true);
			return result;
		}

		/**
		 * Converts the table definition into a result set.
		 * 
		 * @throws RGMAPermanentException
		 */
		private TupleSet convertToResultSet(SchemaTableDefinition tableDef) throws RGMAPermanentException {
			TupleSet rs = new TupleSet();
			String tableName = tableDef.getTableName();
			String viewFor = tableDef.getViewFor();
			List<SchemaColumnDefinition> columns = tableDef.getColumns();

			for (int i = 0; i < columns.size(); i++) {
				SchemaColumnDefinition column = columns.get(i);
				String[] row = new String[] { tableName, column.getName(), column.getType().getType().toString(), column.getType().getSize() + "",
						column.isNotNull() + "", column.isPrimaryKey() + "", viewFor };
				rs.addRow(row);
			}
			rs.setEndOfResults(true);
			return rs;
		}

		private String getVDBName(HttpServletRequest request) throws RGMAPermanentException {
			return getStringParameter(ServletConstants.P_VDB_NAME, request);
		}

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer) throws RGMATemporaryException, RGMAPermanentException,
				UnknownResourceException, IOException {

			if (operation.equals(ServletConstants.M_CREATE_TABLE)) {
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String vdbName = getVDBName(request);
				if (!canForward) {
					Service.checkSystemContext(vdbName, getSystemContext(request));
				}
				String create = getStringParameter(ServletConstants.P_CREATE_TABLE_STATEMENT, request);
				String[] rules = request.getParameterValues(ServletConstants.P_TABLE_AUTHZ_RULE);
				List<String> tableAuthz = new LinkedList<String>();
				if (rules != null) {
					for (String element : rules) {
						tableAuthz.add(element);
					}
				}
				boolean change = m_service.createTable(vdbName, canForward, create, tableAuthz, getUserContext(request));
				writer.writeBoolean(change);

			} else if (operation.equals(ServletConstants.M_DROP_TABLE)) {
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String vdbName = getVDBName(request);
				if (!canForward) {
					Service.checkSystemContext(vdbName, getSystemContext(request));
				}
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				boolean change = m_service.dropTable(vdbName, canForward, tableName, getUserContext(request));
				writer.writeBoolean(change);

			} else if (operation.equals(ServletConstants.M_GET_ALL_TABLES)) {
				List<String> tableNames = new ArrayList<String>();
				tableNames = m_service.getAllTables(getVDBName(request), getUserContext(request));
				writer.writeResultSet(convertTableNamesToResultSet(tableNames));

			} else if (operation.equals(ServletConstants.M_DROP_VIEW)) {
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String vdbName = getVDBName(request);
				if (!canForward) {
					Service.checkSystemContext(vdbName, getSystemContext(request));
				}
				String viewName = getStringParameter(ServletConstants.P_VIEW_NAME, request);
				boolean change = m_service.dropView(vdbName, canForward, viewName, getUserContext(request));
				writer.writeBoolean(change);

			} else if (operation.equals(ServletConstants.M_CREATE_VIEW)) {
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String vdbName = getVDBName(request);
				if (!canForward) {
					Service.checkSystemContext(vdbName, getSystemContext(request));
				}
				String viewStmt = getStringParameter(ServletConstants.P_CREATE_VIEW_STATEMENT, request);
				String[] rules = request.getParameterValues(ServletConstants.P_VIEW_AUTHZ_RULE);
				List<String> viewAuthz = new LinkedList<String>();
				if (rules != null) {
					for (String element : rules) {
						viewAuthz.add(element);
					}
				}
				boolean change = m_service.createView(vdbName, canForward, viewStmt, viewAuthz, getUserContext(request));
				writer.writeBoolean(change);

			} else if (operation.equals(ServletConstants.M_DROP_INDEX)) {
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String vdbName = getVDBName(request);
				if (!canForward) {
					Service.checkSystemContext(vdbName, getSystemContext(request));
				}
				String indexName = getStringParameter(ServletConstants.P_INDEX_NAME, request);
				boolean change = m_service.dropIndex(vdbName, tableName, canForward, indexName, getUserContext(request));
				writer.writeBoolean(change);

			} else if (operation.equals(ServletConstants.M_CREATE_INDEX)) {
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String vdbName = getVDBName(request);
				if (!canForward) {
					Service.checkSystemContext(vdbName, getSystemContext(request));
				}
				String indexStmt = getStringParameter(ServletConstants.P_CREATE_INDEX_STATEMENT, request);
				boolean change = m_service.createIndex(vdbName, canForward, indexStmt, getUserContext(request));
				writer.writeBoolean(change);

			} else if (operation.equals(ServletConstants.M_GET_TABLE_DEFINITION)) {
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				SchemaTableDefinition tableDef = m_service.getTableDefinition(getVDBName(request), tableName, getUserContext(request));
				writer.writeResultSet(convertToResultSet(tableDef));

			} else if (operation.equals(ServletConstants.M_GET_TABLE_INDEXES)) {
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				List<SchemaIndex> indexes = m_service.getTableIndexes(getVDBName(request), tableName, getUserContext(request));
				writer.writeResultSet(convertIndexListToResultSet(indexes));

			} else if (operation.equals(ServletConstants.M_SET_AUTHZ_RULES)) {
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String vdbName = getVDBName(request);
				if (!canForward) {
					Service.checkSystemContext(vdbName, getSystemContext(request));
				}
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				String[] rules = request.getParameterValues(ServletConstants.P_TABLE_AUTHZ_RULE);
				List<String> authz = new LinkedList<String>();
				if (rules != null) {
					for (String element : rules) {
						authz.add(element);
					}
				}
				boolean change = m_service.setAuthorizationRules(vdbName, canForward, tableName, authz, getUserContext(request));
				writer.writeBoolean(change);

			} else if (operation.equals(ServletConstants.M_GET_AUTHZ_RULES)) {
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				List<String> tableAuthz = m_service.getAuthorizationRules(getVDBName(request), tableName, getUserContext(request));
				writer.writeResultSet(convertToResultSet(tableAuthz));

			} else if (operation.equals(ServletConstants.M_GET_ALL_SCHEMA)) {
				List<TupleSet> fullSchema = m_service.getSchemaUpdates(getVDBName(request), 0);
				writer.writeResultSets(fullSchema);

			} else if (operation.equals(ServletConstants.M_GET_SCHEMA_UPDATES)) {
				long tStamp = getLongParameter(ServletConstants.P_TIMESTAMP, request);
				List<TupleSet> fullSchema = m_service.getSchemaUpdates(getVDBName(request), tStamp);
				writer.writeResultSets(fullSchema);

			} else if (operation.equals(ServletConstants.M_ALTER)) {
				boolean canForward = getBooleanParameter(ServletConstants.P_CAN_FORWARD, request);
				String vdbName = getVDBName(request);
				if (!canForward) {
					Service.checkSystemContext(vdbName, getSystemContext(request));
				}
				String torv = getStringParameter(ServletConstants.P_TABLE_OR_VIEW, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
				String action = getStringParameter(ServletConstants.P_ACTION, request);
				String name = getStringParameter(ServletConstants.P_NAME, request);
				String type = request.getParameter(ServletConstants.P_TYPE);
				boolean change = m_service.alter(vdbName, canForward, torv, tableName, action, name, type, getUserContext(request));
				writer.writeBoolean(change);

			} else {
				m_serviceMapper.doOperation(operation, request, writer, m_service);
			}
		}
	}

	private class SecondaryProducerServiceMapper {
		private SecondaryProducerService m_pservice = null;

		SecondaryProducerServiceMapper() throws UnavailableException {
			try {
				m_pservice = SecondaryProducerService.getInstance();
			} catch (RGMAPermanentException e) {
				throw new UnavailableException(e.getFlattenedMessage());
			}
		}

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer) throws RGMATemporaryException, RGMAPermanentException,
				UnknownResourceException, IOException, NumericException {

			if (operation.equals(ServletConstants.M_CREATE_SECONDARYPRODUCER)) {
				boolean isHistory = getBooleanParameter(ServletConstants.P_IS_HISTORY, request);
				boolean isLatest = getBooleanParameter(ServletConstants.P_IS_LATEST, request);
				String logicalName = request.getParameter(ServletConstants.P_LOGICAL_NAME);
				String producerType = getStringParameter(ServletConstants.P_TYPE, request);
				ProducerProperties pProps = null;
				if (producerType.equalsIgnoreCase("MEMORY")) {
					pProps = new ProducerProperties(Storage.MEMORY, isHistory, isLatest);
				} else if (producerType.equalsIgnoreCase("DATABASE")) {
					Storage st = null;
					if (logicalName == null) {
						st = Storage.DATABASE;
					} else {
						st = Storage.getDatabase(logicalName);
					}
					pProps = new ProducerProperties(st, isHistory, isLatest);
				} else {
					throw new RGMAPermanentException("Unsupported Producer Type: " + producerType + " on the " + operation);
				}
				int resourceID = m_pservice.createSecondaryProducer(getUserContext(request), pProps);
				writer.writeInt(resourceID);

			} else if (operation.equals(ServletConstants.M_ADD_PRODUCER)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				{
					String url = getStringParameter(ServletConstants.P_URL, request);
					int id = getIntParameter(ServletConstants.P_ID, request);
					String vdbName = getStringParameter(ServletConstants.P_VDB_NAME, request);
					String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request);
					String predicate = getStringParameter(ServletConstants.P_PREDICATE, request);
					int hrpSec = (int) getLongParameter(ServletConstants.P_HRP_SEC, request);
					ResourceEndpoint re = null;
					try {
						re = new ResourceEndpoint(new URL(url), id);
					} catch (MalformedURLException e) {
						throw new RGMAPermanentException(e);
					}
					ProducerType pt = new ProducerType(getBooleanParameter(ServletConstants.P_IS_HISTORY, request), getBooleanParameter(
							ServletConstants.P_IS_LATEST, request), getBooleanParameter(ServletConstants.P_IS_CONTINUOUS, request), getBooleanParameter(
							ServletConstants.P_IS_STATIC, request), getBooleanParameter(ServletConstants.P_IS_SECONDARY, request));
					ProducerTableEntry producerTable = new ProducerTableEntry(re, vdbName, tableName, pt, hrpSec, predicate);
					m_pservice.addProducer(resourceId, producerTable);
				}
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_REMOVE_PRODUCER)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String url = getStringParameter(ServletConstants.P_URL, request);
				int id = getIntParameter(ServletConstants.P_ID, request);
				ResourceEndpoint producer = null;
				try {
					producer = new ResourceEndpoint(new URL(url), id);
				} catch (MalformedURLException e) {
					throw new RGMAPermanentException(e);
				}
				m_pservice.removeProducer(resourceId, producer);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_DECLARE_TABLE)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				String tableName = getStringParameter(ServletConstants.P_TABLE_NAME, request, null);
				String predicate = getStringParameter(ServletConstants.P_PREDICATE, request, "");
				long hrpSec = getLongParameter(ServletConstants.P_HRP_SEC, request, 0);
				m_pservice.declareTable(getUserContext(request), resourceId, tableName, predicate, (int) hrpSec);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_SHOW_SIGN_OF_LIFE)) {
				int resourceId = getIntParameter(ServletConstants.P_CONNECTION_ID, request);
				m_pservice.showSignOfLife(resourceId);
				writer.writeStatusOK();

			} else {
				m_producerServiceMapper.doOperation(operation, request, writer, m_pservice);
			}
		}
	}

	private class ServiceMapper {

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer, Service service) throws RGMATemporaryException,
				RGMAPermanentException, IOException, UnknownResourceException {

			if (operation.equals(ServletConstants.M_GET_PROPERTY)) {
				String name = getStringParameter(ServletConstants.P_NAME, request);
				String param = request.getParameter(ServletConstants.P_PARAMETER); // May be null
				writer.writeXML(service.getProperty(name, param));

			} else {
				LOG.warn("Unsupported operation: " + operation);
				throw new RGMAPermanentException("Unsupported operation: " + operation);
			}
		}
	}

	private class RGMAMapper {

		void doOperation(String operation, HttpServletRequest request, ServletResponseWriter writer) throws RGMATemporaryException, RGMAPermanentException,
				IOException, UnknownResourceException {

			if (operation.equals(ServletConstants.M_DROP_TUPLE_STORE)) {
				String logicalName = getStringParameter(ServletConstants.P_LOGICAL_NAME, request);
				ProducerService.dropTupleStore(getUserContext(request), logicalName);
				writer.writeStatusOK();

			} else if (operation.equals(ServletConstants.M_LIST_TUPLE_STORES)) {
				TupleSet rs = ProducerService.listTupleStores(getUserContext(request));
				writer.writeResultSet(rs);

			} else if (operation.equals(ServletConstants.M_GET_TERMINATION_INTERVAL)) {
				int terminationIntervalSec = ResourceManagementService.getTerminationInterval();
				writer.writeInt(terminationIntervalSec);

			} else if (operation.equals(ServletConstants.M_GET_VERSION)) {
				String version = Service.getVersion();
				writer.writeString(version);

			} else {
				LOG.warn("Unsupported operation: " + operation);
				throw new RGMAPermanentException("Unsupported operation: " + operation);
			}
		}
	}

	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger("rgma.servlets");

	private static VOMSValidator s_vomsValidator;

	private ConsumerServiceMapper m_consumerServiceMapper;

	private PrimaryProducerServiceMapper m_primaryProducerServiceMapper;

	private SecondaryProducerServiceMapper m_secondaryProducerServiceMapper;

	private OnDemandProducerServiceMapper m_onDemandProducerServiceMapper;

	private RegistryServiceMapper m_registryServiceMapper;

	private SchemaServiceMapper m_schemaServiceMapper;

	private ProducerServiceMapper m_producerServiceMapper;

	private ResourceManagementServiceMapper m_resourceManagementServiceMapper;

	private ServiceMapper m_serviceMapper;

	private RGMAMapper m_rgmaMapper;

	private int s_maximumExpectedResponseTimeMillis;

	private static int s_highestRequestCount;

	private static Object s_requestCountLock = new Object();

	private static int s_currentRequestCount;

	public static int getCurrentRequestCount() {
		synchronized (s_requestCountLock) {
			return s_currentRequestCount;
		}
	}

	public static int getHighestRequestCount() {
		synchronized (s_requestCountLock) {
			return s_highestRequestCount;
		}
	}

	@Override
	public void destroy() {
		try {
			VDBConfigurator.dropInstance();
			ClientAccessConfigurator.dropInstance();

			ConsumerService.dropInstance();
			PrimaryProducerService.dropInstance();
			SecondaryProducerService.dropInstance();
			SchemaService.dropInstance();
			RegistryService.dropInstance();

			TaskManager.dropInstance();
			TupleStoreManager.dropInstance();
			StreamingSender.dropInstance();
			StreamingReceiver.dropInstance();
		} catch (RGMAPermanentException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @see HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		ServletResponseWriter writer = new ServletResponseWriter(response);

		String operation = request.getPathInfo();

		if (operation != null) {
			while (operation.contains("/")) {
				operation = operation.substring(1);
			}
		} else {
			RGMAPermanentException e = new RGMAPermanentException("No operation found in request:");
			LOG.warn("Error processing servlet request. " + e.getMessage());
			writer.writeException(e);
			writer.close();
			return;
		}

		String serviceName = request.getRequestURI().toLowerCase();
		long startTime = System.currentTimeMillis();
		synchronized (s_requestCountLock) {
			s_currentRequestCount++;
			s_highestRequestCount = Math.max(s_highestRequestCount, s_currentRequestCount);
		}
		try {
			if (serviceName.contains("consumerservlet")) {
				m_consumerServiceMapper.doOperation(operation, request, writer);
			} else if (serviceName.contains("primaryproducerservlet")) {
				m_primaryProducerServiceMapper.doOperation(operation, request, writer);
			} else if (serviceName.contains("secondaryproducerservlet")) {
				m_secondaryProducerServiceMapper.doOperation(operation, request, writer);
			} else if (serviceName.contains("ondemandproducerservlet")) {
				m_onDemandProducerServiceMapper.doOperation(operation, request, writer);
			} else if (serviceName.contains("registryservlet")) {
				m_registryServiceMapper.doOperation(operation, request, writer);
			} else if (serviceName.contains("schemaservlet")) {
				m_schemaServiceMapper.doOperation(operation, request, writer);
			} else if (serviceName.contains("rgmaservice")) {
				m_rgmaMapper.doOperation(operation, request, writer);
			} else {
				LOG.info("Received request that can't be handled " + request.toString());
			}
			int delay = (int) (System.currentTimeMillis() - startTime);
			if (delay > s_maximumExpectedResponseTimeMillis) {
				synchronized (s_requestCountLock) {
					LOG.warn("Request " + request.getRequestURI() + " took " + delay + " milliseconds. " + s_currentRequestCount + " requests in progress.");
				}
			}
		} catch (UnknownResourceException e) {
			writer.writeException(e);
		} catch (RGMATemporaryException e) {
			writer.writeException(e);
		} catch (RGMAPermanentException e) {
			writer.writeException(e);
		} catch (NumericException e) {
			writer.writeException(e);
		} catch (Throwable t) {
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			PrintWriter output = new PrintWriter(buffer);
			/* The message is printed as well as logged in case logging is not properly setup */
			t.printStackTrace(output);
			output.close();
			RGMAPermanentException e = new RGMAPermanentException(buffer.toString());
			LOG.error(buffer);
			writer.writeException(e);
		} finally {
			writer.close();
			synchronized (s_requestCountLock) {
				s_currentRequestCount--;
			}
		}
	}

	/**
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest,
	 *      javax.servlet.http.HttpServletResponse)
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		doGet(request, response);
	}

	/**
	 * Initialize the R-GMA server. Sets the system properties RGMA_HOME, TRUSTFILE, configures log4j logging,
	 * initalizes resource manager and operation authorizer.
	 * 
	 * @see GenericServlet#init()
	 */
	@Override
	public void init() throws UnavailableException {
		// RGMA_HOME needs to be set in the CATALINA_OPTS not from the web.xml
		String rgmaHome = System.getProperty(ServerConstants.RGMA_HOME_PROPERTY);
		String log4jFile = rgmaHome + ServerConstants.LOG4J_LOCATION;
		PropertyConfigurator.configureAndWatch(log4jFile, ServerConstants.LOG4J_INTERVAL_MILLIS);
		// check to see if the X509_USER_PROXY variable is set if not log it and throw unavailable
		// exception
		try {
			ServerConfig sc = ServerConfig.getInstance();
			sc.getString(ServerConstants.SERVLETCONNECTION_X509_USER_PROXY);
			sc.getString(ServerConstants.SERVLETCONNECTION_X509_CERT_DIR);
			s_maximumExpectedResponseTimeMillis = sc.getInt(ServerConstants.SERVER_MAXIMUM_EXPECTED_RESPONSE_TIME_MILLIS);

		} catch (RGMAPermanentException e) {
			StringBuilder msg = new StringBuilder("Error occured in Servlet startup ");
			msg.append(e.getMessage());
			LOG.error(msg);
			throw new UnavailableException(msg.toString());
		}
		s_vomsValidator = new VOMSValidator((X509Certificate) null);
		s_currentRequestCount = 0;
		try {
			/* create service mappers to deal with requests */
			m_consumerServiceMapper = new ConsumerServiceMapper();
			m_primaryProducerServiceMapper = new PrimaryProducerServiceMapper();
			m_secondaryProducerServiceMapper = new SecondaryProducerServiceMapper();
			m_onDemandProducerServiceMapper = new OnDemandProducerServiceMapper();
			m_registryServiceMapper = new RegistryServiceMapper();
			m_schemaServiceMapper = new SchemaServiceMapper();

			/* create other mappers to provide common functionality */
			m_producerServiceMapper = new ProducerServiceMapper();
			m_resourceManagementServiceMapper = new ResourceManagementServiceMapper();
			m_serviceMapper = new ServiceMapper();
			m_rgmaMapper = new RGMAMapper();
		} catch (UnavailableException e) {
			StringBuilder msg = new StringBuilder("Error occured in Servlet startup ");
			msg.append(e.getMessage());
			if (e.getCause() != null) {
				msg.append(" Caused by " + e.getCause().getMessage());
			}
			LOG.error(msg);
			/* The message is printed as well as logged in case logging is not properly setup */
			System.out.println("*** " + msg + " ***");
			throw e;
		}
	}

	private ResourceEndpoint decodeResourceEndpoint(String connectionString) throws RGMAPermanentException {
		int n = connectionString.indexOf(" ");
		String idString = connectionString.substring(0, n);
		String servlet = connectionString.substring(n + 1);
		try {
			return new ResourceEndpoint(new URL(servlet), Integer.parseInt(idString));
		} catch (NumberFormatException e) {
			throw new RGMAPermanentException("Invalid resource ID parameter: " + idString);
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException("Invalid URL parameter: " + servlet);
		}
	}

	private boolean getBooleanParameter(String name, HttpServletRequest request) throws RGMAPermanentException {
		String s = request.getParameter(name);

		if (s != null) {
			if (s.equalsIgnoreCase("TRUE")) {
				return true;
			} else if (s.equalsIgnoreCase("FALSE")) {
				return false;
			} else {
				throw new RGMAPermanentException("Boolean parameter " + name + " had invalid value: " + s);
			}
		}

		throw new RGMAPermanentException("Required parameter " + name + " not found");
	}

	private UserSystemContext getUserSystemContext(HttpServletRequest request) {
		/*
		 * Obtain user DN and FQNS and ClientHostName from the request object and get the connectingHostName
		 */
		String userDN = request.getParameter(ServletConstants.P_USER_DN);
		String clientHostName = request.getParameter(ServletConstants.P_CLIENT_HOST_NAME);
		String realClientHostName = request.getRemoteHost().toUpperCase();
		List<FQAN> fqans = new ArrayList<FQAN>();
		String[] fqanStrings = request.getParameterValues(ServletConstants.P_FQAN);
		if (fqanStrings != null) {
			for (String fqanString : fqanStrings) {
				fqans.add(new FQAN(fqanString));
			}
		}
		return new UserSystemContext(userDN, fqans, clientHostName, realClientHostName);
	}

	private UserContext getUserContext(HttpServletRequest request) {
		/*
		 * Obtain context information from the certificate
		 */
		X509Certificate[] certificates = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
		Set<String> oids = certificates[0].getCriticalExtensionOIDs();
		String proxyOid = "1.3.6.1.4.1.3536.1.222";
		String userDN = (oids.contains(proxyOid)) ? certificates[0].getIssuerX500Principal().getName() : certificates[0].getSubjectX500Principal().getName();
		String clientHostName = request.getRemoteHost().toUpperCase();
		s_vomsValidator.setClientChain(certificates);
		s_vomsValidator.validate();
		String[] fqanStrings = s_vomsValidator.getAllFullyQualifiedAttributes();
		List<FQAN> fqans = new ArrayList<FQAN>();
		for (String fqanString : fqanStrings) {
			fqans.add(new FQAN(fqanString));
		}
		return new UserContext(userDN, fqans, clientHostName);
	}

	private UserSystemContext getSystemContext(HttpServletRequest request) {
		/*
		 * Obtain minimal context information for a non-user related system call
		 */
		String connectingHostName = request.getRemoteHost().toUpperCase();
		return new UserSystemContext(null, null, null, connectingHostName);
	}

	private int getIntParameter(String name, HttpServletRequest request) throws RGMAPermanentException, RGMAPermanentException {
		String s = request.getParameter(name);

		if (s != null) {
			try {
				return Integer.parseInt(s.trim());
			} catch (NumberFormatException e) {
				throw new RGMAPermanentException("Integer parameter " + name + " had invalid value: " + s);
			}
		}

		throw new RGMAPermanentException("Required parameter " + name + " not found");
	}

	private long getLongParameter(String name, HttpServletRequest request) throws RGMAPermanentException, RGMAPermanentException {
		String s = request.getParameter(name);

		if (s != null) {
			try {
				return Long.parseLong(s.trim());
			} catch (Exception e) {
				throw new RGMAPermanentException("Long parameter " + name + " had invalid value: " + s);
			}
		}

		throw new RGMAPermanentException("Required parameter " + name + " not found");
	}

	private long getLongParameter(String name, HttpServletRequest request, long defaultValue) throws RGMAPermanentException, RGMAPermanentException {
		if (!parameterExists(name, request)) {
			return defaultValue;
		} else {
			return getLongParameter(name, request);
		}
	}

	private String getStringParameter(String name, HttpServletRequest request) throws RGMAPermanentException {
		String value = getStringParameter(name, request, null);

		if (value != null) {
			return value;
		} else {
			throw new RGMAPermanentException("Required parameter not found: " + name);
		}
	}

	private String getStringParameter(String name, HttpServletRequest request, String defaultValue) {
		String s = request.getParameter(name);

		if (s != null) {
			return s;
		} else {
			return defaultValue;
		}
	}

	private boolean parameterExists(String name, HttpServletRequest request) {
		String value = request.getParameter(name);

		return value != null;
	}
}
