/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;


/**
 * A resource endpoint contains a URL and a resource ID.
 */
public class ResourceEndpoint {

	/** URLString of resource */
    private String m_urlString;

    /** ID of resource. */
    private int m_resourceId;
    
    /**
     * Creates a new resource endpoint object.
     *
     * @param urlString the service URL
     * @param resourceId the resource identifier
     */
     public ResourceEndpoint(String urlString, int resourceId) {
    	m_urlString = urlString;
        m_resourceId = resourceId;
    }

    /**
     * Gets the resource identifier.
     *
     * @return the resourceId
     */
    public int getResourceId() {
        return m_resourceId;
    }

    /**
     * Gets the resource URL.
     *
     * @return the resource URL as a string
     */
	public String getUrlString() {
		return m_urlString;
	}
	
	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 * 
	 * @see Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer("ResourceEndpoint[");
		str.append(m_urlString).append(":").append(m_resourceId).append("]");
		return str.toString();
	}	
}
