/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

import java.net.URL;

import org.glite.rgma.server.services.ServerConstants;

/**
 * A resource endpoint, containing a URL and a resource ID.
 */
public class ResourceEndpoint {
	/** ID of resource. */
	private int m_resourceID;

	/** URL. */
	private URL m_URL;

	public ResourceEndpoint(URL url, int resourceId) throws RGMAPermanentException {
		if (url == null) {
			throw new RGMAPermanentException("May not create an endpoint with null URL and resourceId " + resourceId);
		}
		m_resourceID = resourceId;
		m_URL = url;
	}

	/**
	 * Compares this object with the specified object.
	 * 
	 * @param obj
	 *            Object to compare.
	 * @return <code>true</code> if this object and the specified object are equal.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ResourceEndpoint) {
			ResourceEndpoint e = (ResourceEndpoint) obj;
			return e.getResourceID() == m_resourceID && e.getURL().equals(m_URL);
		}
		return false;
	}

	/**
	 * Gets the resource ID.
	 * 
	 * @return The resource identifier.
	 */
	public int getResourceID() {
		return m_resourceID;
	}

	/**
	 * Gets the resource URL.
	 * 
	 * @return The resource URL.
	 */
	public URL getURL() {
		return m_URL;
	}

	@Override
	public int hashCode() {
		return m_resourceID;
	}

	/**
	 * Returns a String representation of this ResourceEndpoint.
	 * 
	 * @return String representation of this ResourceEndpoint.
	 */
	@Override
	public String toString() {
		if (m_URL != null) {
			String path = m_URL.getPath();
			String rt = null;
			if (path.endsWith(ServerConstants.CONSUMER_SERVICE_NAME)) {
				rt = "C";
			} else if (path.endsWith(ServerConstants.PRIMARY_PRODUCER_SERVICE_NAME)) {
				rt = "PP";
			} else if (path.endsWith(ServerConstants.SECONDARY_PRODUCER_SERVICE_NAME)) {
				rt = "SP";
			} else if (path.endsWith(ServerConstants.ONDEMAND_PRODUCER_SERVICE_NAME)) {
				rt = "ODP";
			}
			return "[" + rt + ":" + m_URL.getHost() + "/" + m_resourceID + "]";
		}
		return "[null/" + m_resourceID + "]";
	}
}
