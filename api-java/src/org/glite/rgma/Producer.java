package org.glite.rgma;

/**
 * A producer.
 */
public class Producer extends Resource {

	/**
	 * Return the resource endpoint to be used in the constructor of a consumer to make a "directed query".
	 * 
	 * @return the resource endpoint
	 */
	public ResourceEndpoint getResourceEndpoint() {
		try {
			return new ResourceEndpoint(ServletConnection.getServerCommonURL() + m_servletName, m_resourceId);
		} catch (RGMAPermanentException e) {
			/* This exception cannot occur because if a concrete resource exists then it must have a URL. */
			return null;
		}
	}

	protected Producer() {}
}
