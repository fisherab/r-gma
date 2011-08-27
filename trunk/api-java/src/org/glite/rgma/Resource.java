/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma;

/**
 * A producer or consumer.
 */
public abstract class Resource {

	/** Resource ID. */
	protected int m_resourceId;

	/** Resource URL */
	protected String m_servletName;
	
	/** To record that a resource is closed and so should not be remade on the server */
	private boolean m_dead;

	protected ServletConnection getNewConnection() throws RGMAPermanentException {
		if (m_dead) {
			throw new RGMAPermanentException("This resource cannot be reused after you have closed or destroyed it.");
		}
		ServletConnection sc = new ServletConnection(m_servletName);
		sc.addParameter("connectionId", m_resourceId);
		return sc;
	}
	
	protected Resource() {
	}

	/**
	 * Closes the resource. The resource will no longer be available through this API and will be
	 * destroyed once it has finished interacting with other components. It is not an error if the
	 * resource does not exist.
	 * 
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 * @see #destroy()
	 */
	public void close() throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(m_servletName);
		connection.addParameter("connectionId", m_resourceId);
		checkOK(connection.sendNonResourceCommand("close"));
		m_dead = true;
	}

	/**
	 * Closes and destroys the resource. The resource will no longer be available to any component.
	 * It is not an error if the resource does not exist.
	 * 
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 * @see #close()
	 */
	public void destroy() throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(m_servletName);
		connection.addParameter("connectionId", m_resourceId);
		checkOK(connection.sendNonResourceCommand("destroy"));
		m_dead = true;
	}
	
	static void checkOK(TupleSet ts) throws RGMAPermanentException {
		if (ts.getData().get(0).getString(0).equals("OK")) {
			return;
		}
		throw new RGMAPermanentException("Failed to return status of OK");
	}

}
