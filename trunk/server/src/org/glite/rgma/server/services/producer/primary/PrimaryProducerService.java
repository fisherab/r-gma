/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.primary;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteProducer;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.producer.ProducerResource;
import org.glite.rgma.server.services.producer.ProducerService;
import org.glite.rgma.server.services.producer.store.TupleStoreManager;
import org.glite.rgma.server.services.resource.Resource;
import org.glite.rgma.server.system.ProducerProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;

/**
 * An instance of this class, which occurs as a singleton, handles all the request designated for the primary producer
 * service.
 */
public class PrimaryProducerService extends ProducerService {

	private static final Object s_instanceLock = new Object();

	/** A static reference to an instances of this class. */
	private static PrimaryProducerService s_service;

	public static void dropInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_service != null) {
				s_service.shutdown();
				s_service = null;
			}
		}
	}

	public static PrimaryProducerService getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_service == null) {
				s_service = new PrimaryProducerService();
				String idFilename = m_config.getString(ServerConstants.PRIMARY_PRODUCER_ID_FILE);
				s_service.setResourceIdFileName(idFilename);
			}
		}
		return s_service;
	}

	private URL m_URL;

	/**
	 * Creates a new PrimaryProducerService object.
	 * 
	 * @throws ConfigurationException
	 *             DOCUMENT ME!
	 * @throws RGMAPermanentException
	 */
	private PrimaryProducerService() throws RGMAPermanentException {
		super(ServerConstants.PRIMARY_PRODUCER_SERVICE_NAME);
		try {
			m_logger = Logger.getLogger(PrimaryProducerConstants.PRIMARY_PRODUCER_LOGGER);
			m_URL = new URL(getURLString());
			PrimaryProducerResource.setStaticVariables(this, getRegistryTerminationInterval(), getHostname());
			RemoteProducer.setStaticVariables(this);
			TupleStoreManager.setPrimaryProducerService(this);
		} catch (MalformedURLException e) {
			m_logger.error("Malformed URL Exception", e);
			throw new RGMAPermanentException("PrimaryProducerService has not started - malformed URL Exception " + e.getMessage());
		} catch (RGMAPermanentException e) {
			m_logger.fatal("PrimaryProducerService has not started" + e.getFlattenedMessage());
			throw e;
		}
		s_controlLogger.info("PrimaryProducerService started");
	}

	/**
	 * Creates a new primary producer resource.
	 */
	public int createPrimaryProducer(UserContext userContext, ProducerProperties properties) throws RGMAPermanentException, RGMAPermanentException,
			RGMAPermanentException, RGMATemporaryException {
		try {
			checkBusy();
			s_taskManager.checkBusy();
			Service.checkUserContext(userContext);
			checkOnline();
			int resourceId = getNextResourceId();
			ResourceEndpoint endpoint = new ResourceEndpoint(m_URL, resourceId);
			ProducerResource producer = new PrimaryProducerResource(userContext, properties, endpoint);
			addResource(producer);
			if (m_logger.isInfoEnabled()) {
				m_logger.info("Primary Producer resource: " + endpoint.getResourceID() + " created");
			}
			return endpoint.getResourceID();
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to create primary producer resource " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to create primary producer resource " + e.getFlattenedMessage());
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
	 * @throws RGMAPermanentException
	 */
	public void declareTable(UserContext userContext, int resourceId, String tableName, String predicate, int hrpSecs, int lrpSecs)
			throws RGMAPermanentException, UnknownResourceException, RGMAPermanentException, RGMAPermanentException {
		try {
			PrimaryProducerResource producer = (PrimaryProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			producer.declareTable(userContext, tableName, predicate, hrpSecs, lrpSecs);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to declare table: " + tableName + " from primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to declare table: " + tableName + " from primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Declared Table :" + tableName + " in primary resource: " + resourceId);
		}
	}

	/**
	 * Returns the latest retention period for the specified table.
	 * 
	 * @param resourceId
	 *            The id of the resource publishing to the specified table
	 * @param tableName
	 *            Table to retrieve its LRP.
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 * @throws UnknownResourceException
	 */
	public int getLatestRetentionPeriod(int resourceId, String tableName) throws RGMAPermanentException, RGMAPermanentException, UnknownResourceException {
		try {
			PrimaryProducerResource producer = (PrimaryProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			int lrpsecs = producer.getLatestRetentionPeriod(tableName);
			if (m_logger.isInfoEnabled()) {
				m_logger.info("Returned LRP for primary resource: " + resourceId);
			}
			return lrpsecs;
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to get Latest Retention Period from primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to get Latest Retention Period from primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
	}

	/**
	 * Returns properties for the given property name
	 * 
	 * @throws UnknownResourceException
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
	 * Inserts a tuple into the tuple store for the specified primary producer resource with default lrp secs.
	 */
	public void insert(int resourceId, String insertString, UserContext context) throws RGMAPermanentException, UnknownResourceException,
			RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		try {
			checkBusy();
			PrimaryProducerResource producer = (PrimaryProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			producer.insert(context, insertString, 0);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to Insert Tuple into primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to Insert Tuple into primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to Insert Tuple into primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Inserted tuple into primary resource: " + resourceId);
		}
	}

	/**
	 * Inserts a tuple into the tuple store of the specified primary producer resource.
	 */
	public void insert(int resourceId, String insertString, int lrpSec, UserContext context) throws RGMAPermanentException, UnknownResourceException,
			RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		/* TODO this should be deleted and insertlist renamed to insert - but a lot of internal tests will break ... */
		try {
			checkBusy();
			if (lrpSec <= 0) { // This check is outside the resource so that zero can be used as a
				// flag
				throw new RGMAPermanentException("LRP must be > 0 and not " + lrpSec + " seconds");
			}
			PrimaryProducerResource producer = (PrimaryProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			producer.insert(context, insertString, lrpSec);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to insert tuple into primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to insert tuple into primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to Insert Tuple into primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Inserted tuple into primary resource: " + resourceId);
		}
	}

	/**
	 * Inserts a number of tuples into the tuple store of the specified primary producer resource.
	 */
	public void insertList(int resourceId, List<String> insertString, UserContext userContext) throws RGMAPermanentException, UnknownResourceException,
			RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		try {
			checkBusy();
			PrimaryProducerResource producer = (PrimaryProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			producer.insertList(userContext, insertString, 0);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to Insert Tuples: from primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to Insert Tuples: from primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to Insert Tuple into primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Inserted " + insertString.size() + " tuples into primary resource: " + resourceId);
		}
	}

	/**
	 * Inserts a number of tuples into the tuple store of the specified primary producer resource.
	 */
	public void insertList(int resourceId, List<String> insertString, int lrpSec, UserContext userContext) throws RGMAPermanentException,
			UnknownResourceException, RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		try {
			checkBusy();
			/* This check is outside the resource so that zero can be used as a flag */
			if (lrpSec <= 0) {
				throw new RGMAPermanentException("LRP must be > 0 and not " + lrpSec + " seconds");
			}
			PrimaryProducerResource producer = (PrimaryProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			producer.insertList(userContext, insertString, lrpSec);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to Insert Tuples: from primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to Insert Tuples: from primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to Insert Tuple into primary resource: " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Inserted " + insertString.size() + " tuples into primary resource: " + resourceId);
		}
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
			boolean fullDetails = false;
			Resource[] allResources = getSomeResources(100);
			for (Resource r : allResources) {
				PrimaryProducerResource resource = (PrimaryProducerResource) r;
				boolean tableDetails = false;
				boolean queryDetails = false;
				b.append(resource.getDetails(tableDetails, queryDetails, fullDetails));
			}
			b.append("</Producer>");
			result = wrapData(b.toString());

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_STATUS)) {
			try {
				int numParam = Integer.parseInt(param);
				PrimaryProducerResource resource = (PrimaryProducerResource) getResource(numParam);
				boolean tableDetails = true;
				boolean queryDetails = false;
				boolean fulltableDetails = true;
				result = resource.getDetails(tableDetails, queryDetails, fulltableDetails);
				result = wrapData(result);
			} catch (NumberFormatException nmr) {
				throw new RGMAPermanentException("Invalid parameter: expected 'connectionId' (integer), " + "received " + param);
			}

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_TABLE_STATUS)) {
			try {
				int numParam = Integer.parseInt(param);
				PrimaryProducerResource resource = (PrimaryProducerResource) getResource(numParam);
				boolean tableDetails = true;
				boolean queryDetails = true;
				boolean fulltableDetails = true;
				result = resource.getDetails(tableDetails, queryDetails, fulltableDetails);
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