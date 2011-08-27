/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.resource;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.UserContext;

/**
 * Base class for all R-GMA Resources. Resources are identified by a ResourceEndpoint, a combination of a URL and an
 * integer resource ID.
 * <p>
 * Resources use 'soft-state' registration and will be automatically closed and destroyed if they are not contacted by a
 * user for a time greater than their termination interval.
 */
public abstract class Resource {
	/**
	 * Status codes for a resource
	 */
	protected enum Status {
		/** Just created but not yet ready for use */
		NEW,
		/** Ready for use */
		ACTIVE,
		/** May not be contacted via the User API */
		CLOSED,
		/** May not be contacted via any API */
		DESTROYED,
	}

	public String displayStatus() {
		synchronized (m_status) {
			return m_status.toString();
		}
	}

	public String toString() {
		return m_endpoint.toString();
	}

	/** Resource endpoint. */
	protected final ResourceEndpoint m_endpoint;

	/** SecurityContext from user's call creating this resource */
	protected final UserContext m_context;

	/** Current status. */
	protected Status m_status;

	/** Time of most recent user API call. */
	protected long m_lastContactTime;

	private long m_timeCreated;

	protected final String m_rgmaHome;

	protected final Logger m_logger;

	private static TaskManager s_taskManager;

	private static int s_terminationIntervalSec;

	/**
	 * @param endpoint
	 * @param terminationIntervalSec
	 * @param userContext
	 * @param logger
	 * @throws RGMAPermanentException
	 */
	public Resource(ResourceEndpoint endpoint, UserContext userContext, Logger logger) throws RGMAPermanentException {
		m_logger = logger;
		m_rgmaHome = System.getProperty(ServerConstants.RGMA_HOME_PROPERTY);
		m_endpoint = endpoint;
		m_context = userContext;
		m_timeCreated = m_lastContactTime = System.currentTimeMillis();
		m_status = Status.NEW; // Actual resources must set to ACTIVE once fully initialised.
	}

	/**
	 * Get the endpoint of this resource.
	 * 
	 * @return ResourceEndpoint for the resource.
	 */
	public final ResourceEndpoint getEndpoint() {
		return m_endpoint;
	}

	/**
	 * Get the Distinguished Name of the user who created this resource.
	 * 
	 * @return User's DN from the original certificate.
	 */
	public final String getCreatorDn() {
		return m_context.getDN();
	}

	/**
	 * Get time in ms since last contact. This is the most recent contact any user API method (including showSignOfLife)
	 * was called on this resource.
	 */
	public final long getLastContactIntervalMillis() {
		return System.currentTimeMillis() - m_lastContactTime;
	}

	/**
	 * Update the last contact time for the resource. Sets the last contact time to the current system time.
	 */
	public final void updateLastContactTime() {
		m_lastContactTime = System.currentTimeMillis();
	}

	/**
	 * Close the resource The resource will no longer be contactable via the User API. It may still interact with the
	 * system API.
	 * 
	 * @throws RGMAPermanentException
	 */
	public final void close() throws RGMAPermanentException {
		synchronized (m_status) {
			if ((m_status == Status.CLOSED) || (m_status == Status.DESTROYED)) {
				return;
			}
			m_status = Status.CLOSED;
		}
		if (canDestroy()) {
			destroy();
		}
	}

	/**
	 * Check if this resource is still active. The resource must not be inactive, closed or destroyed.
	 */
	public final boolean isActive() {
		synchronized (m_status) {
			return m_status == Status.ACTIVE;
		}
	}

	/**
	 * Check if this resource has been closed.
	 */
	public final boolean isClosed() {
		synchronized (m_status) {
			return m_status == Status.CLOSED;
		}
	}

	/**
	 * Test if this resource can be destroyed. This should be overridden by concrete subclasses to determine if it is
	 * safe to destroy a closed resource. For example a PrimaryProducerResource must return <code>false</code> until it
	 * no longer holds any tuples which have not been streamed or are within the history retention period.
	 * 
	 * @return <code>true</code> if the resource is closed and is no longer needed by other components.
	 * @throws RGMAPermanentException
	 */
	public abstract boolean canDestroy() throws RGMAPermanentException;

	/**
	 * Destroy the resource The resource will no be contactable by any components. This method should be overridden by
	 * the concrete subclass to free any resources used by the resource and set the status to DESTROYED.
	 * 
	 * @throws RGMAPermanentException
	 */
	public abstract void destroy() throws RGMAPermanentException;

	/**
	 * Check if this resource has been destroyed.
	 */
	public final boolean isDestroyed() {
		synchronized (m_status) {
			return m_status == Status.DESTROYED;
		}
	}

	/**
	 * Check if this resource has expired. A resource expires when its last contact time plus its termination interval
	 * is less than the current system time.
	 */
	public final boolean hasExpired() {
		long terminationTime = m_lastContactTime + s_terminationIntervalSec * 1000;
		return terminationTime < System.currentTimeMillis();
	}

	/**
	 * Update the registry entry for this resource Concrete subclasses should implements this to update their
	 * registration. This method should not block - registration should be asynchronous.
	 */
	public abstract void updateRegistry();

	/**
	 * Fetches the address of the client that created this resource
	 * 
	 * @return A string representing the address of the client that created this resource
	 */
	public String getClientHostName() {
		return m_context.getHostName();
	}

	/**
	 * Fetches the time when this resource was created
	 * 
	 * @return Time in milliseconds since 1970 when the resource was created
	 */
	public long getTimeCreated() {
		return m_timeCreated;
	}

	protected int getTerminationInterval() {
		return s_terminationIntervalSec;
	}

	protected void checkContext(UserContext context) throws RGMAPermanentException {
		if (!context.getDN().equals(m_context.getDN())) {
			throw new RGMAPermanentException("DN does not match that of the resource");
		}
	}

	/**
	 * @return A string representation of the tasks belonging to this resource (in XML).
	 */
	protected String getTasksDisplay() {
		StringBuilder b = new StringBuilder();
		String ownerId = getEndpoint().toString();
		List<Map<String, String>> tasks = s_taskManager.getTasks(ownerId);
		for (Map<String, String> task : tasks) {
			b.append("<Task ");
			for (String key : task.keySet()) {
				b.append(key).append("=\"").append(task.get(key)).append("\" ");
			}
			b.append("/>\n");
		}
		return b.toString();
	}

	public static void setStaticVariables() throws RGMAPermanentException {
		s_taskManager = TaskManager.getInstance();
		s_terminationIntervalSec = ResourceManagementService.getTerminationInterval();
	}

}
