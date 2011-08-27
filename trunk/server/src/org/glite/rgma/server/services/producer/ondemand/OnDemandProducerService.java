package org.glite.rgma.server.services.producer.ondemand;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLSocketFactory;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteProducer;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.producer.ProducerResource;
import org.glite.rgma.server.services.producer.ProducerService;
import org.glite.rgma.server.services.resource.Resource;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserSystemContext;
import org.glite.security.trustmanager.ContextWrapper;
import org.glite.security.util.CaseInsensitiveProperties;

public class OnDemandProducerService extends ProducerService {

	private static final Object s_instanceLock = new Object();

	/** A static reference to an instances of this class. */
	private static OnDemandProducerService s_service;

	public static void dropInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_service != null) {
				s_service.shutdown();
				s_service = null;
			}
		}
	}

	public static OnDemandProducerService getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_service == null) {
				s_service = new OnDemandProducerService();
				String idFilename = m_config.getString(ServerConstants.ONDEMAND_PRODUCER_ID_FILE);
				s_service.setResourceIdFileName(idFilename);
			}
		}
		return s_service;
	}

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
			boolean fullDetails = false;
			for (Resource r : allResources) {
				OnDemandProducerResource resource = (OnDemandProducerResource) r;
				boolean tableDetails = false;
				boolean queryDetails = false;
				b.append(resource.getDetails(tableDetails, queryDetails, fullDetails));
			}
			b.append("</Producer>");
			result = wrapData(b.toString());

		} else if (name.equalsIgnoreCase(ServerConstants.RESOURCE_STATUS)) {
			try {
				int numParam = Integer.parseInt(param);
				OnDemandProducerResource resource = (OnDemandProducerResource) getResource(numParam);
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
				OnDemandProducerResource resource = (OnDemandProducerResource) getResource(numParam);
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

	private URL m_URL;

	private OnDemandProducerService() throws RGMAPermanentException {
		super(ServerConstants.ONDEMAND_PRODUCER_SERVICE_NAME);
		try {
			m_logger = Logger.getLogger(OnDemandProducerConstants.ONDEMAND_PRODUCER_LOGGER);
			m_URL = new URL(getURLString());

			/* Set up the SSLSocketFactory */
			ServerConfig serverConfig = ServerConfig.getInstance();
			String proxyfile = serverConfig.getString(ServerConstants.SERVLETCONNECTION_X509_USER_PROXY);
			m_logger.debug("X509_USER_PROXY: " + proxyfile);
			String cadir = serverConfig.getString(ServerConstants.SERVLETCONNECTION_X509_CERT_DIR);
			m_logger.debug("X509_CERT_DIR: " + cadir);
			CaseInsensitiveProperties trustproperties = new CaseInsensitiveProperties();
			if (new File(proxyfile).exists()) {
				trustproperties.setProperty("gridProxyFile", proxyfile);
			} else {
				trustproperties.setProperty("sslCertFile", "/etc/grid-security/hostcert.pem");
				trustproperties.setProperty("sslKey", "/etc/grid-security/hostkey.pem");
			}
			trustproperties.setProperty("sslCaFiles", cadir + File.separator + "*.0");
			trustproperties.setProperty("crlFiles", cadir + File.separator + "*.r0");
			trustproperties.setProperty("credentialsUpdateInterval", "1h");
			SSLSocketFactory sslSocketFactory = null;
			try {
				ContextWrapper contextSSL = new ContextWrapper(trustproperties);
				sslSocketFactory = contextSSL.getSocketFactory();
			} catch (IOException e) {
				throw new RGMAPermanentException(e);
			} catch (GeneralSecurityException e) {
				throw new RGMAPermanentException(e);
			}

			OnDemandProducerResource.setStaticVariables(this, getRegistryTerminationInterval(), getHostname(), sslSocketFactory);
			RemoteProducer.setStaticVariables(this);

		} catch (MalformedURLException e) {
			m_logger.error("Malformed URL Exception", e);
			throw new RGMAPermanentException("OnDemandProducerService has not started - malformed URL Exception " + e.getMessage());
		} catch (RGMAPermanentException e) {
			m_logger.fatal("OnDemandProducerService has not started" + e.getFlattenedMessage());
			throw e;
		}
		s_controlLogger.info("OnDemandProducerService started");
	}

	public int createOnDemandProducer(UserContext userContext, String hostName, int port) throws RGMAPermanentException, RGMAPermanentException,
			RGMAPermanentException, RGMATemporaryException {
		ProducerResource producer = null;
		ResourceEndpoint endpoint = null;
		try {
			checkBusy();
			s_taskManager.checkBusy();
			Service.checkUserContext(userContext);
			checkOnline();
			int resourceId = getNextResourceId();
			endpoint = new ResourceEndpoint(m_URL, resourceId);
			producer = new OnDemandProducerResource(userContext, hostName, port, endpoint);
			addResource(producer);
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to create onDemand producer resource " + e.getFlattenedMessage());
			throw e;
		} catch (RGMATemporaryException e) {
			m_logger.info("Failed to create onDemand producer resource " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("OnDemand Producer resource " + endpoint.getResourceID() + " created for " + hostName + ":" + port);
		}
		return endpoint.getResourceID();
	}

	public void declareTable(UserContext userContext, int resourceId, String tableName, String predicate) throws UnknownResourceException,
			RGMAPermanentException, RGMAPermanentException, RGMAPermanentException {
		try {
			OnDemandProducerResource producer = (OnDemandProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			producer.declareTable(userContext, tableName, predicate);
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to declare table " + tableName + " from OnDemand resource " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Failed to declare table " + tableName + " from OnDemand resource " + resourceId + " - " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Declared Table " + tableName + " in OnDemand resource " + resourceId);
		}
	}

	public TupleSet start(int resourceId, String select, QueryProperties queryProps, TimeInterval timeout, ResourceEndpoint consumer,
			StreamingProperties streamingProps, UserSystemContext context) throws UnknownResourceException, RGMAPermanentException, RGMAPermanentException,
			NumericException {
		return super.start(resourceId, select, queryProps, timeout, consumer, streamingProps, context);
	}

}
