package org.glite.rgma.server.services.producer.secondary;

import java.net.URL;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteConsumable;
import org.glite.rgma.server.remote.RemoteProducer;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.producer.ProducerResource;
import org.glite.rgma.server.services.producer.ProducerService;
import org.glite.rgma.server.services.producer.store.TupleStoreManager;
import org.glite.rgma.server.services.resource.Resource;
import org.glite.rgma.server.system.ProducerProperties;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;

public class SecondaryProducerService extends ProducerService {

	private static Object s_instanceLock = new Object();

	/** A static reference to an instant of this class. */
	private static SecondaryProducerService s_service;

	private URL m_URL;

	private SecondaryProducerService() throws RGMAPermanentException {
		super(ServerConstants.SECONDARY_PRODUCER_SERVICE_NAME);
		try {
			m_logger = Logger.getLogger(SecondaryProducerConstants.SECONDARY_PRODUCER_LOGGER);
			m_URL = getURL();
			SecondaryProducerResource.setStaticVariables(this, getRegistryTerminationInterval());
			RemoteProducer.setStaticVariables(this);
			RemoteConsumable.setStaticVariables(this);
			TupleStoreManager.setSecondaryProducerService(this);
		} catch (RGMAPermanentException e) {
			m_logger.fatal("SecondaryProducerService has not started" + e.getMessage());
			throw new RGMAPermanentException(e.getMessage());
		}
		s_controlLogger.info("SecondaryProducerService started");
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 * @throws ConfigurationException
	 *             DOCUMENT ME!
	 * @throws RGMAPermanentException
	 */
	public static SecondaryProducerService getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_service == null) {
				String idFilename;
				s_service = new SecondaryProducerService();
				idFilename = m_config.getString(ServerConstants.SECONDARY_PRODUCER_ID_FILE);
				s_service.setResourceIdFileName(idFilename);
			}
			return s_service;
		}
	}

	public static void dropInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_service != null) {
				s_service.shutdown();
				s_service = null;
			}
		}
	}

	/**
	 * Creates a new secondary producer resource.
	 */
	public int createSecondaryProducer(UserContext userContext, ProducerProperties properties) throws RGMAPermanentException, RGMAPermanentException,
			RGMATemporaryException, RGMAPermanentException {
		try {
			checkBusy();
			s_taskManager.checkBusy();
			Service.checkUserContext(userContext);
			checkOnline();
			int resourceId = getNextResourceId();
			ResourceEndpoint endpoint = new ResourceEndpoint(m_URL, resourceId);
			ProducerResource producer = new SecondaryProducerResource(userContext, properties, endpoint);
			addResource(producer);
			if (m_logger.isInfoEnabled()) {
				m_logger.info("Created Producer resource successfully.... Endpoint = " + producer.getEndpoint());
			}
			return endpoint.getResourceID();
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to create secondary producer resource " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to create secondary producer resource " + e.getFlattenedMessage());
			throw e;
		}
	}

	/**
	 * Adds a table to the list of tables to which this producer may publish tuples.
	 * 
	 * @param resourceId
	 *            The id of the resource wanting to use the specified table
	 * @param tableName
	 *            Table to register.
	 * @param predicate
	 *            The where clause of this producer
	 * @throws RGMAPermanentException
	 * @throws UnknownResourceException
	 * @throws RGMAPermanentException
	 * @throws UnknownResourceException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 */
	public void declareTable(UserContext userContext, int resourceId, String tableName, String predicate, int hrpSecs) throws RGMAPermanentException,
			UnknownResourceException, RGMAPermanentException, RGMAPermanentException {
		try {
			if (hrpSecs < 0) {
				throw new RGMAPermanentException("HRP must be >=0 and not " + hrpSecs);
			}
			SecondaryProducerResource producer = (SecondaryProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			producer.declareTable(userContext, tableName, predicate, hrpSecs);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to declare table: " + tableName + " from primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to declare table: " + tableName + " from primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Declared Table :" + tableName + " from primary resource: " + resourceId);
		}
	}

	public void addProducer(int resourceId, ProducerTableEntry producerTable) throws UnknownResourceException, RGMAPermanentException {

		try {
			if (!producerTable.getProducerType().isContinuous()) {
				throw new RGMAPermanentException("Invalid producer type, expected: continuous, found: " + producerTable.getProducerType());
			}
			m_logger.debug("Received a valid addProducer request from: " + producerTable);
			SecondaryProducerResource secProducer = (SecondaryProducerResource) getResource(resourceId);
			checkContactable(secProducer, Api.SYSTEM_API);
			secProducer.addProducer(producerTable);
			m_logger.debug("Added a new producer " + producerTable + " to secondaryProducer: " + resourceId);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to add producer: " + producerTable.getEndpoint() + " to secondary producer resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to add producer: " + producerTable.getEndpoint() + " to secondary producer resource: " + resourceId + " - "
					+ e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isDebugEnabled()) {
			m_logger.debug("Added producer: " + producerTable.getEndpoint() + " to secondary producer resource: " + resourceId);
		}
	}

	public void removeProducer(int resourceId, ResourceEndpoint producer) throws UnknownResourceException, RGMAPermanentException {

		try {
			SecondaryProducerResource secProducer = (SecondaryProducerResource) getResource(resourceId);
			checkContactable(secProducer, Api.SYSTEM_API);
			secProducer.removeProducer(producer);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to remove producer: " + producer + " from consumer resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to remove producer: " + producer + " from consumer resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Removed producer: " + producer + " from secondary producer resource: " + resourceId);
		}
	}

	public final void showSignOfLife(int resourceId) throws UnknownResourceException, RGMAPermanentException {
		try {
			Resource resource = getResource(resourceId);
			checkContactable(resource, Api.USER_API);
		} catch (UnknownResourceException e) {
			m_logger.info("ShowSignOfLife of " + displayResource(resourceId) + " failed " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("ShowSignOfLife of " + displayResource(resourceId) + " failed " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info(displayResource(resourceId) + " received showSignOfLife");
		}
	}

	/**
	 * Returns properties for the given property name
	 * 
	 * @throws UnknownResourceException
	 * @throws UnknownResourceException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 */
	public String getProperty(String name, String param) throws UnknownResourceException, RGMAPermanentException, RGMAPermanentException {
		String result = null;
		try {
			try {
				result = getServiceProperty(name, param);
			} catch (RGMAPermanentException e) {
				result = getProp(name, param);
			}
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to get property: " + name + ":" + param + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to get property: " + name + ":" + param + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isDebugEnabled()) {
			m_logger.debug("getProperty: " + name + ":" + param + " starts " + result.substring(0, 20));
		}
		return result;
	}

	/**
	 * Returns properties specific to the producer service
	 * 
	 * @throws UnknownResourceException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 */
	private String getProp(String name, String param) throws UnknownResourceException, RGMAPermanentException, RGMAPermanentException {
		String result = null;

		char q = '\"';
		if (name.equalsIgnoreCase(ServerConstants.SERVICE_RESOURCES)) {
			StringBuilder b = new StringBuilder();
			b.append("<Producer MaxTermIntervalMillis=").append(q).append(-1).append(q);
			b.append(" MinTermIntervalMillis=").append(q).append(-1).append(q);
			b.append(" LocalUpdateIntervalMillis=").append(q).append(m_localUpdateInterval).append(q);
			b.append(" RemoteUpdateIntervalMillis=").append(q).append(m_remoteUpdateInterval).append(q);
			b.append(" ResourceCount=").append(q).append(getResourceCount()).append(q);
			b.append(">\n");
			Resource[] allResources = getSomeResources(100);

			for (Resource r : allResources) {
				SecondaryProducerResource resource = (SecondaryProducerResource) r;
				boolean tableDetails = false;
				boolean queryDetails = false;
				boolean fullDetails = false;
				b.append(resource.getDetails(tableDetails, queryDetails, fullDetails));
			}

			b.append("</Producer>");

			result = wrapData(b.toString());

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_STATUS)) {
			try {
				int numParam = Integer.parseInt(param);
				SecondaryProducerResource resource = (SecondaryProducerResource) getResource(numParam);
				boolean tableDetails = true;
				boolean queryDetails = false;
				boolean fullDetails = true;
				result = resource.getDetails(tableDetails, queryDetails, fullDetails);
				result = wrapData(result);
			} catch (NumberFormatException nmr) {
				throw new RGMAPermanentException("Invalid parameter: expected 'connectionId' (integer), " + "received " + param);
			}

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_TABLE_STATUS)) {
			try {
				int numParam = Integer.parseInt(param);
				SecondaryProducerResource resource = (SecondaryProducerResource) getResource(numParam);
				boolean tableDetails = true;
				boolean queryDetails = true;
				boolean fullDetails = true;
				result = resource.getDetails(tableDetails, queryDetails, fullDetails);
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
