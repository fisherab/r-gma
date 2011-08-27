/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.consumer;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteConsumable;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.database.MySQLConnection;
import org.glite.rgma.server.services.resource.Resource;
import org.glite.rgma.server.services.resource.ResourceManagementService;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;

/**
 * Consumer service which executes requests on local consumer resources.
 */
public class ConsumerService extends ResourceManagementService {

	private static Object s_instanceLock = new Object();

	// These are set by getInstance and the constructor
	private static ConsumerService s_consumerService;

	private URL m_URL;

	private ConsumerService() throws RGMAPermanentException {
		super(ServerConstants.CONSUMER_SERVICE_NAME);
		try {
			m_logger = Logger.getLogger(ConsumerConstants.CONSUMER_LOGGER);
			MySQLConnection.init();
			TupleQueue.dropOldTables(ConsumerConstants.TUPLEQUEUE_TABLENAME_PREFIX);
			try {
				m_URL = new URL(getURLString());
			} catch (MalformedURLException e) {
				throw new RGMAPermanentException("This cannot happen", e);
			}
			ConsumerResource.setStaticVariables(this, getRegistryTerminationInterval());
			RemoteConsumable.setStaticVariables(this);
		} catch (RGMAPermanentException e) {
			m_logger.fatal("ConsumerService has not started " + e.getFlattenedMessage());
			throw e;
		}
		s_controlLogger.info("ConsumerService started");
	}

	public static ConsumerService getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_consumerService == null) {
				s_consumerService = new ConsumerService();
				String idFilename = m_config.getString(ServerConstants.CONSUMER_ID_FILE);
				s_consumerService.setResourceIdFileName(idFilename);
			}
			return s_consumerService;
		}
	}

	public static void dropInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_consumerService != null) {
				s_consumerService.shutdown();
				s_consumerService = null;
			}
		}
	}

	public int createConsumer(UserContext userContext, String select, QueryProperties queryProperties, TimeInterval timeout, List<ResourceEndpoint> producers)
			throws RGMAPermanentException, RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		try {
			checkBusy();
			s_taskManager.checkBusy();
			ConsumerResource consumer;
			ResourceEndpoint endpoint;
			Service.checkUserContext(userContext);
			checkOnline();
			endpoint = new ResourceEndpoint(m_URL, getNextResourceId());
			consumer = new ConsumerResource(userContext, select, queryProperties, endpoint, timeout, producers);
			addResource(consumer);
			if (m_logger.isInfoEnabled()) {
				m_logger.info("Created " + displayResource(endpoint.getResourceID()));
			}
			return endpoint.getResourceID();
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to create consumer resource " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to create consumer resource " + e.getFlattenedMessage());
			throw e;
		}

	}

	public void abort(int resourceId) throws UnknownResourceException, RGMAPermanentException, RGMAPermanentException {
		try {
			ConsumerResource consumer = (ConsumerResource) getResource(resourceId);
			checkContactable(consumer, Api.USER_API);
			consumer.abort();
		} catch (UnknownResourceException e) {
			m_logger.info("Abort of consumer resource: " + resourceId + " failed " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Abort of consumer resource: " + resourceId + " failed " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Consumer resource: " + resourceId + " aborted");
		}
	}

	public boolean hasAborted(int resourceId) throws UnknownResourceException, RGMAPermanentException {
		boolean aborted;
		try {
			ConsumerResource consumer = (ConsumerResource) getResource(resourceId);
			checkContactable(consumer, Api.USER_API);
			aborted = consumer.hasAborted();
		} catch (UnknownResourceException e) {
			m_logger.info("hasAborted on consumer resource = " + resourceId + " failed " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("hasAborted on consumer resource = " + resourceId + " failed " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Consumer resource: " + resourceId + " hasAborted: " + aborted);
		}
		return aborted;
	}

	public TupleSet pop(UserContext userContext, int resourceId, int maxCount) throws UnknownResourceException, RGMATemporaryException, RGMAPermanentException {

		TupleSet result;
		try {
			ConsumerResource consumer = (ConsumerResource) getResource(resourceId);
			checkContactable(consumer, Api.USER_API);
			result = consumer.pop(userContext, maxCount);
		} catch (UnknownResourceException e) {
			m_logger.info("Pop from consumer resource: " + resourceId + " failed " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Pop from consumer resource: " + resourceId + " failed " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Pop " + result.size() + " tuples from consumer resource: " + resourceId);
		}
		return result;
	}

	public void addProducer(int resourceId, ProducerTableEntry producerTable) throws UnknownResourceException, RGMAPermanentException {

		try {
			if (!producerTable.getProducerType().isContinuous()) {
				throw new RGMAPermanentException("Invalid producer type, expected: continuous, found: " + producerTable.getProducerType());
			}
			m_logger.debug("Received a valid addProducer request from: " + producerTable);
			ConsumerResource consumer = (ConsumerResource) getResource(resourceId);
			checkContactable(consumer, Api.SYSTEM_API);
			consumer.addProducer(producerTable);
			m_logger.debug("Added a new producer " + producerTable + " to consumer: " + resourceId);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to add producer: " + producerTable.getEndpoint() + " to unknown consumer resource: " + resourceId);
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to add producer: " + producerTable.getEndpoint() + " to consumer resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Added producer: " + producerTable.getEndpoint() + " to consumer resource: " + resourceId);
		}
	}

	public void removeProducer(int resourceId, ResourceEndpoint producer) throws UnknownResourceException, RGMAPermanentException {

		try {
			ConsumerResource consumer = (ConsumerResource) getResource(resourceId);
			checkContactable(consumer, Api.SYSTEM_API);
			if (!(consumer.m_queryProperties.isContinuous())) {
				if (m_logger.isInfoEnabled()) {
					m_logger.info("Removed producer: " + producer + " from consumer resource: " + resourceId + " was ignored for one-off query");
				}
				return;
			}
			consumer.removeProducer(producer);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to remove producer: " + producer + " from unknown consumer resource: " + resourceId);
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to remove producer: " + producer + " from consumer resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Removed producer: " + producer + " from consumer resource: " + resourceId);
		}
	}

	public String getProperty(String name, String param) throws UnknownResourceException, RGMAPermanentException, RGMAPermanentException {
		String result = null;
		try {
			try {
				result = getServiceProperty(name, param);
			} catch (RGMAPermanentException e) {
				result = getProp(name, param);
			}
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to get property: " + name + ":" + param + " - " + e.getMessage());
			throw e;
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to get property: " + name + ":" + param + " - " + e.getMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("getProperty: " + name + ":" + param);
		}
		return result;
	}

	/**
	 * Returns properties specific to the consumer service
	 * 
	 * @throws RGMAPermanentException
	 * @throws UnknownResourceException
	 * @throws RGMAPermanentException
	 */
	private String getProp(String name, String param) throws RGMAPermanentException, UnknownResourceException, RGMAPermanentException {
		String result = null;

		char q = '\"';
		if (name.equalsIgnoreCase(ServerConstants.SERVICE_RESOURCES)) {
			StringBuilder b = new StringBuilder();
			b.append("<Consumer MaxTermIntervalMillis=").append(q).append(-1).append(q);
			b.append(" MinTermIntervalMillis=").append(q).append(-1).append(q);
			b.append(" LocalUpdateIntervalMillis=").append(q).append(m_localUpdateInterval).append(q);
			b.append(" RemoteUpdateIntervalMillis=").append(q).append(m_remoteUpdateInterval).append(q);
			b.append(" ResourceCount=").append(q).append(getResourceCount()).append(q);
			b.append(">\n");

			Resource[] allResources = getSomeResources(100);
			boolean fullDetails = false;

			for (Resource r : allResources) {
				ConsumerResource resource = (ConsumerResource) r;
				b = b.append(resource.getDetails(fullDetails));
			}

			b.append("</Consumer>");

			result = wrapData(b.toString());

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_STATUS)) {

			try {
				int numParam = Integer.parseInt(param);
				ConsumerResource resource = (ConsumerResource) getResource(numParam);
				boolean fullDetails = true;
				result = resource.getDetails(fullDetails);
				result = wrapData(result);
			} catch (NumberFormatException nmr) {
				throw new RGMAPermanentException("Invalid parameter: expected 'connectionId' (integer), " + "received " + param);
			}
		} else {
			throw new RGMAPermanentException("Unrecognised property name: " + name);
		}
		return result;
	}

}
