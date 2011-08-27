/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.resource;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.Units;
import org.glite.rgma.server.system.UnknownResourceException;

/**
 * Resource service which executes requests on local Resource objects.. Base class for local Consumer and Producer
 * services.
 */
public class ResourceManagementService extends Service {

	/**
	 * TimerTask to update the registration of the resource locally in this manager.
	 */
	private class LocalUpdateTask extends TimerTask {
		private final Resource m_resource;

		public LocalUpdateTask(Resource resource) {
			m_resource = resource;
		}

		/**
		 * If the resource has expired close it. Then if it is closed and can be destroyed, destroy it (via abstract
		 * destroy method) and remove from the manager's resources. If it has been destroyed by the user it will still
		 * be known to the resource manager and must just be removed from the manager's resources.
		 */

		@Override
		public void run() {
			if (m_resource.hasExpired()) {
				try {
					m_resource.close();
				} catch (RGMAPermanentException e) {
					m_logger.error(e);
					remove();
				}
			}

			if (m_resource.isClosed()) {
				try {
					if (m_resource.canDestroy()) {
						m_resource.destroy();
						remove();
					}
				} catch (RGMAPermanentException e) {
					m_logger.error(e);
					remove();
				}
			} else if (m_resource.isDestroyed()) {
				/* Has been destroyed by the user. */
				remove();
			}
		}

		private void remove() {
			m_resources.remove(m_resource.getEndpoint());
			cancel();
		}

	}

	/**
	 * TimerTask to update the registration of the resource remotely.
	 */
	private class RemoteUpdateTask extends TimerTask {
		private final Resource m_resource;

		public RemoteUpdateTask(Resource resource) {
			m_resource = resource;
		}

		/**
		 * If the resource is no longer alive, cancel the task, otherwise send an update
		 */
		@Override
		public void run() {
			if (m_resource.isClosed() || m_resource.isDestroyed()) {
				cancel();
			} else {
				m_resource.updateRegistry();
			}
		}

	}

	protected static ServerConfig m_config;

	public static int getTerminationInterval() {
		return s_termIntervalSec;
	}

	protected final long m_localUpdateInterval;

	protected final long m_remoteUpdateInterval;

	private final String m_id;

	/** Holds the value of the next available resource id */
	private final Properties m_idProps = new Properties();

	/**
	 * Value of present ID
	 */
	private int m_idValue;

	/** Timer thread that updates the registration of resources locally */
	private final Timer m_localUpdateTimer;

	/**
	 * Interval between recording ID values to a file
	 */
	private final int m_recordingInterval;

	private final TimeInterval m_registryTerminationInterval;

	/** Timer thread that updates the registration of resources remotely */
	private final Timer m_remoteUpdateTimer;

	/** Name of file used to store next available resource ID */
	private String m_resourceIdFilename;

	private final long m_resourceRegistryLatency;

	/** List of active resources */
	private Map<ResourceEndpoint, Resource> m_resources;

	private URL m_URL;

	private long m_initialRemoteUpdateInterval;

	private static int s_termIntervalSec;

	protected ResourceManagementService(String name) throws RGMAPermanentException {
		// Note that this is not surrounded by a try clause for logging as this will be done
		// by a class extending it
		super(name);
		m_config = getServerConfig();
		s_termIntervalSec = m_config.getInt(ServerConstants.RESOURCE_TERM_INTERVAL_SECS);
		m_localUpdateInterval = m_config.getLong(ServerConstants.RESOURCE_LOCAL_UPDATE_INTERVAL_SECS) * 1000;
		m_initialRemoteUpdateInterval = (m_config.getLong(ServerConstants.REGISTRY_REPLICATION_INTERVAL_SECS) + m_config
				.getLong(ServerConstants.REGISTRY_REPLICATION_LAG_SECS)) * 1000;
		m_remoteUpdateInterval = m_config.getLong(ServerConstants.RESOURCE_REMOTE_UPDATE_INTERVAL_SECS) * 1000;
		m_resourceRegistryLatency = m_config.getInt(ServerConstants.RESOURCE_REGISTRY_LATENCY_SECS);
		m_registryTerminationInterval = new TimeInterval(m_config.getInt(ServerConstants.RESOURCE_REMOTE_UPDATE_INTERVAL_SECS) + m_resourceRegistryLatency,
				Units.SECONDS);
		m_id = ServerConstants.RESOURCE_ID;
		m_recordingInterval = m_config.getInt(ServerConstants.RESOURCE_ID_RECORDING_INTERVAL_COUNT);

		m_localUpdateTimer = new Timer(m_serviceName + "LocalUpdateTimer", true);
		m_remoteUpdateTimer = new Timer(m_serviceName + "RemoteUpdateTimer", true);
		m_resources = Collections.synchronizedMap(new HashMap<ResourceEndpoint, Resource>());
		try {
			m_URL = new URL(getURLString());
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException("Valid URL has not been set up: " + getURLString());
		}

		// Sanity check
		if (m_resourceRegistryLatency >= s_termIntervalSec) {
			throw new RGMAPermanentException(ServerConstants.RESOURCE_REGISTRY_LATENCY_SECS + ": " + m_resourceRegistryLatency + " is not less than "
					+ ServerConstants.RESOURCE_TERM_INTERVAL_SECS + ": " + s_termIntervalSec);
		}
	}

	public final void close(int resourceId) throws RGMAPermanentException {
		try {
			checkOnline();
			Resource resource = null;
			try {
				resource = getResource(resourceId);
			} catch (UnknownResourceException e) {}
			if (resource != null) {
				resource.close();
			}
		} catch (RGMAPermanentException e) {
			m_logger.error("Close of " + displayResource(resourceId) + " failed " + e.getMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info(displayResource(resourceId) + " closed");
		}
	}

	public final void destroy(int resourceId) throws RGMAPermanentException {
		try {
			checkOnline();
			Resource resource = null;
			try {
				resource = getResource(resourceId);
			} catch (UnknownResourceException e) {}
			if (resource != null) {
				resource.destroy();
			}
		} catch (RGMAPermanentException e) {
			m_logger.error("Destroy of resource: " + resourceId + " failed " + e.getMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info(m_URL.toString() + " " + resourceId + " destroyed");
		}
	}

	@Override
	public String getProperty(String name, String param) throws UnknownResourceException, RGMAPermanentException {
		return null;
	}

	public final Resource getResource(int resourceId) throws UnknownResourceException, RGMAPermanentException {
		ResourceEndpoint endpoint = new ResourceEndpoint(m_URL, resourceId);
		Resource resource = m_resources.get(endpoint);
		if (resource == null) {
			throw new UnknownResourceException(endpoint);
		}
		return resource;
	}

	public final int getTerminationInterval(int resourceId) throws RGMAPermanentException, UnknownResourceException {
		int ti = 0;
		try {
			Resource resource;
			resource = getResource(resourceId);
			checkContactable(resource, Api.USER_API);
			ti = resource.getTerminationInterval();
		} catch (UnknownResourceException e) {
			m_logger.info("getTerminationInterval of " + displayResource(resourceId) + " failed " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("getTerminationInterval of " + displayResource(resourceId) + " failed " + e.getMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Termination interval for resourceId = " + resourceId + " returned: " + ti);
		}
		return ti;
	}

	public final void ping(int resourceId) throws UnknownResourceException, RGMAPermanentException {
		try {
			Resource resource = getResource(resourceId);
			checkContactable(resource, Api.SYSTEM_API);
		} catch (UnknownResourceException e) {
			m_logger.info("Ping of " + displayResource(resourceId) + " failed");
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Ping of " + displayResource(resourceId) + " failed " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info(displayResource(resourceId) + " pinged");
		}
	}

	/**
	 * Add a resource to be managed.
	 */
	protected final void addResource(Resource resource) throws RGMAPermanentException {
		m_resources.put(resource.getEndpoint(), resource);
		m_localUpdateTimer.schedule(new LocalUpdateTask(resource), m_localUpdateInterval, m_localUpdateInterval);
		m_remoteUpdateTimer.schedule(new RemoteUpdateTask(resource), m_initialRemoteUpdateInterval, m_remoteUpdateInterval);
	}

	/**
	 * Check if a resource can be contacted.
	 * 
	 * @param resource
	 *            The resource to check.
	 * @param systemApi
	 *            If <code>true</code> contact is via System API, otherwise contact is via User API..
	 * @throws RGMAPermanentException
	 * @throw UnknownResourceException If the resource cannot be contacted.
	 */
	protected final void checkContactable(Resource resource, Api api) throws UnknownResourceException, RGMAPermanentException {
		checkOnline();
		if (resource.hasExpired()) {
			resource.close();
		}

		switch (api) {
		case SYSTEM_API:
			// Resources can be contacted by the System API after they are closed.
			if (!resource.isActive() && !resource.isClosed()) {
				throw new UnknownResourceException(resource.getEndpoint());
			}
			break;
		case USER_API:
			// Resources cannot be contacted by the User API after they are closed.
			if (!resource.isActive()) {
				throw new UnknownResourceException(resource.getEndpoint());
			} else {
				resource.updateLastContactTime();
			}
			break;
		}
	}

	protected String displayResource(int resourceId) {
		return "[" + m_URL + ":" + resourceId + "]";
	}

	protected Resource[] getAllResources() {
		return m_resources.values().toArray(new Resource[0]);
	}

	protected Resource[] getSomeResources(int n) {
		List<Resource> some = new ArrayList<Resource>(n);
		synchronized (m_resources) {
			for (Resource r : m_resources.values()) {
				if (n-- <= 0) {
					break;
				}
				some.add(r);
			}
		}
		return some.toArray(new Resource[0]);
	}

	/**
	 * Generates the next available resource Id To allow restart of the sequence the ID is stored periodically. This
	 * whole method is synchronized on m_idProps to ensure that there are no contention problems.
	 */
	protected final int getNextResourceId() {
		synchronized (m_idProps) {
			m_idValue = (m_idValue + 1) % Integer.MAX_VALUE;
			if (m_idValue % m_recordingInterval == 0) {
				m_idProps.setProperty(m_id, String.valueOf(m_idValue));
				try {
					m_idProps.store(new FileOutputStream(m_resourceIdFilename), null);
				} catch (IOException e) {
					m_logger.error("Error writing Resource Id file: " + m_resourceIdFilename + " - " + e);
				}
			}
			return m_idValue;
		}
	}

	protected TimeInterval getRegistryTerminationInterval() {
		return m_registryTerminationInterval;
	}

	protected int getResourceCount() {
		return m_resources.size();
	}

	protected void setResourceIdFileName(String idFilename) {
		m_resourceIdFilename = idFilename;
		try {
			m_idProps.load(new FileInputStream(m_resourceIdFilename));

			String resourceId = m_idProps.getProperty(m_id);

			if (resourceId == null) {
				throw new Exception();
			}

			m_idValue = Integer.parseInt(resourceId);
			if (m_idValue < Integer.MAX_VALUE - m_recordingInterval) {
				m_idValue = m_idValue + m_recordingInterval - 1;
			} else {
				m_idValue = m_recordingInterval - 1;
			}

		} catch (Exception e) {
			s_controlLogger.warn(m_serviceName + " has problem getting id value from file " + m_resourceIdFilename);
			m_idValue = (new Random()).nextInt(Integer.MAX_VALUE);
		}
		s_controlLogger.info("Resource id value for " + m_serviceName + " initialised to " + m_idValue);
	}

	protected void shutdown() throws RGMAPermanentException {
		setOffline("Service has been shutdown");
		m_localUpdateTimer.cancel();
		m_remoteUpdateTimer.cancel();
		for (Resource r : getAllResources()) {
			r.destroy();
		}
	}

}
