/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

abstract class VDB {
	
	  /** Resource ID. */
    protected String m_vdbName;
    
    /** Resource URL */
    protected String m_servletName;
    
    /**
     * Returns a new ServletConnection object to contact this resource.
     *
     * @return A new ServletConnection object using this resource's URL and ID.
     * @throws RGMAPermanentException 
     *
     * @throws RGMAException
     */
    protected ServletConnection getNewConnection() throws RGMAPermanentException  {
        ServletConnection sc = new ServletConnection(m_servletName);
        sc.addParameter("vdbName", m_vdbName);
        return sc;
    }

}
