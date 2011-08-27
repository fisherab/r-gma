/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.servlets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import org.apache.log4j.Logger;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;

/**
 * A servlet-based connection to a remote R-GMA service. Supports GET and POST requests but wit
 */
public class ServletConnection {
	public enum HttpMethod {
		GET, POST, POST_WITH_ENCODED_PARAMS;
	}

	/** Initialise the logging facility */
	private static Logger LOG = Logger.getLogger("rgma.servlets");

	/** URL to connect to */
	private URL m_url;

	/** Http transport method to be used */
	private HttpMethod m_method;

	/** The POST message to be contained in the Http body */
	private String m_postMessage;

	/** Buffer to store all parameters used when creating the URL */
	private StringBuffer m_encodedParams;

	/** The Id of the sender */
	private int m_connectionId;

	/** Object to read the XML response */
	private ServletResponseReader m_responseReader;

	/** True if init() has not been called */
	private static boolean s_notReady = true;

	public synchronized static void init() throws RGMAPermanentException {
		/*
		 * Creates an SSLSocketFactory using the given properties and sets this as the default for HttpsURLConnection.
		 * Also sets the default hostname verifier to always return <code>true</code>.
		 */
		SSLSocketFactory sslSocketFactory = RGMAContextWrapper.getInstance().getSocketFactory();
		HttpsURLConnection.setDefaultSSLSocketFactory(sslSocketFactory);
		HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
			public boolean verify(String urlHostname, SSLSession certHostname) {
				return true;
			}
		});
		s_notReady = false;
	}

	/**
	 * Replaces special characters with hex representation as "%XX".
	 * 
	 * @param value
	 *            String to encode.
	 * @return The encoded string.
	 */
	private static String encodeForPost(String value) {
		// % %25
		value = value.replace("%", "%25");

		// + %2B
		value = value.replace("+", "%2B");

		// & %26
		value = value.replace("&", "%26");

		// = %3D
		value = value.replace("=", "%3D");

		// : %3A
		value = value.replace(":", "%3A");

		// / %2F
		value = value.replace("/", "%2F");

		return value;
	}

	/**
	 * Create a connection to a resource.
	 * 
	 * @param endpoint
	 *            Resource endpoint.
	 * @throws RGMAPermanentException
	 */
	public ServletConnection(ResourceEndpoint endpoint) throws RGMAPermanentException {
		this(HttpMethod.GET);
		m_url = endpoint.getURL();
		m_connectionId = endpoint.getResourceID();
		addParameter("connectionId", m_connectionId);
	}

	/**
	 * Create a connection to a resource.
	 * 
	 * @param endpoint
	 *            Resource endpoint.
	 * @param method
	 *            HTTP method to use for the connection.
	 * @throws RGMAPermanentException
	 */
	public ServletConnection(ResourceEndpoint endpoint, HttpMethod method) throws RGMAPermanentException {
		this(method);
		m_url = endpoint.getURL();
		m_connectionId = endpoint.getResourceID();
		addParameter("connectionId", m_connectionId);
	}

	/**
	 * Create a connection to a service.
	 * 
	 * @param url
	 *            Service url.
	 * @throws RGMAPermanentException
	 */
	public ServletConnection(URL url) throws RGMAPermanentException {
		this(HttpMethod.GET);
		m_url = url;
		m_connectionId = -1;
	}

	/**
	 * Create a connection to a service.
	 * 
	 * @param url
	 *            Service url.
	 * @param method
	 *            HTTP method to use for the connection.
	 * @throws RGMAPermanentException
	 */
	public ServletConnection(URL url, HttpMethod method) throws RGMAPermanentException {
		this(method);
		m_url = url;
		m_connectionId = -1;
	}

	/**
	 * Creates a new ServletConnection object.
	 * 
	 * @param method
	 *            HTTP method (POST or GET).
	 * @throws RGMAPermanentException
	 */
	private ServletConnection(HttpMethod method) throws RGMAPermanentException {
		if (s_notReady) {
			throw new RGMAPermanentException("init has not been called on the ServletConnection");
		}
		m_responseReader = new ServletResponseReader();
		m_encodedParams = new StringBuffer(50);
		m_method = method;
	}

	/**
	 * Add a boolean parameter.
	 * 
	 * @param name
	 *            name of parameter
	 * @param value
	 *            value of parameter
	 */
	public void addParameter(String name, boolean value) {
		addParameter(name, value + "");
	}

	/**
	 * Add a double parameter.
	 * 
	 * @param name
	 *            name of parameter
	 * @param value
	 *            value of parameter
	 */
	public void addParameter(String name, double value) {
		addParameter(name, String.valueOf(value));
	}

	/**
	 * Add an integer parameter.
	 * 
	 * @param name
	 *            name of parameter
	 * @param value
	 *            value of parameter
	 */
	public void addParameter(String name, int value) {
		addParameter(name, String.valueOf(value));
	}

	/**
	 * Add a long parameter.
	 * 
	 * @param name
	 *            name of parameter
	 * @param value
	 *            value of parameter
	 */
	public void addParameter(String name, long value) {
		addParameter(name, String.valueOf(value));
	}

	/**
	 * Add a string parameter
	 * 
	 * @param name
	 *            name of parameter
	 * @param value
	 *            value of parameter (cannot be null).
	 */
	public void addParameter(String name, String value) {
		String escapedValue = null;

		if (m_method == HttpMethod.GET) {
			try {
				escapedValue = URLEncoder.encode(value, "UTF-8");
			} catch (java.io.UnsupportedEncodingException e) {
				// This will never be thrown as UTF-8 should be supported.
				LOG.error("UTF-8 encoding not supported by JVM");
				escapedValue = value;
			}
		} else {
			escapedValue = encodeForPost(value);
		}

		if (name.equals("connectionId")) {
			m_connectionId = new Integer(value).intValue();
		}

		if (m_encodedParams.length() == 0) {
			m_encodedParams.append(name + "=" + escapedValue);
		} else {
			m_encodedParams.append("&" + name + "=" + escapedValue);
		}
	}

	/**
	 * @see java.lang.Object#equals(Object object)
	 */
	public boolean equals(Object object) {
		if (object instanceof ServletConnection) {
			ServletConnection sc = (ServletConnection) object;

			if (m_connectionId == sc.m_connectionId) {
				return m_url.equals(sc.m_url);
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return m_url.hashCode() + m_connectionId;
	}

	public TupleSet sendCommand(String command) throws RemoteException, RGMATemporaryException, RGMAPermanentException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing command " + command + " on " + m_url);
		}
		String xml = connect(command);
		TupleSet result;
		try {
			result = m_responseReader.readResultSet(xml);
		} catch (NumericException e) {
			throw new RGMAPermanentException(e.getMessage() + " number: " + e.getNumber());
		} catch (UnknownResourceException e) {
			throw new RGMAPermanentException(e.getMessage());
		}

		return result;
	}

	public List<TupleSet> sendCommandForMultipleResultSets(String command) throws RemoteException, RGMATemporaryException, RGMAPermanentException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing command " + command + " on " + m_url);
		}
		String xml = connect(command);
		List<TupleSet> result;
		try {
			result = m_responseReader.readResultSets(xml);
		} catch (NumericException e) {
			throw new RGMAPermanentException(e.getMessage() + " number: " + e.getNumber());
		} catch (UnknownResourceException e) {
			throw new RGMAPermanentException(e.getMessage());
		}

		return result;
	}

	public TupleSet sendCommandWithNumericException(String command) throws RemoteException, RGMATemporaryException, RGMAPermanentException, NumericException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing command " + command + " on " + m_url);
		}
		String xml = connect(command);
		try {
			return m_responseReader.readResultSet(xml);
		} catch (UnknownResourceException e) {
			throw new RGMAPermanentException(e.getMessage());
		}
	}

	public TupleSet sendCommandWithUnknowResourceException(String command) throws RemoteException, RGMATemporaryException, RGMAPermanentException,
			UnknownResourceException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing command " + command + " on " + m_url);
		}
		String xml = connect(command);
		try {
			return m_responseReader.readResultSet(xml);
		} catch (NumericException e) {
			throw new RGMAPermanentException(e.getMessage() + " number: " + e.getNumber());
		}
	}

	public TupleSet sendCommandWithAllExceptions(String command) throws RemoteException, RGMATemporaryException, RGMAPermanentException,
			UnknownResourceException, NumericException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing command " + command + " on " + m_url);
		}
		String xml = connect(command);
		return m_responseReader.readResultSet(xml);
	}

	/** Execute a command using the current parameters. */
	public String sendXMLCommand(String command) throws RGMAPermanentException, RemoteException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing command " + command + " on " + m_url);
		}
		return connect(command);
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return m_url + "?" + m_encodedParams;
	}

	/**
	 * Connect using the specified operation.
	 * 
	 * @param operation
	 *            name of operation the servlet is to perform. The operation name is appended to the base servlet URI to
	 *            which are appended any parameters to form the complete URI.
	 * @return XML string
	 * @throws RGMAException
	 * @throws ConnectionTimedOutException
	 * @throws RemoteException
	 * @throws RGMAException
	 */
	private String connect(String operation) throws RemoteException, RGMAPermanentException {
		String responseMessage = "";
		URL url = null;

		try {
			url = getConnectionUrl(operation);
			LOG.debug("Connecting to " + url);
			responseMessage = connect(url);
			return responseMessage;
		} catch (SocketTimeoutException e) {
			throw new RemoteException(e);
		} catch (SSLException e) {
			throw new RGMAPermanentException("SSL error: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new RemoteException(e);
		}
	}

	/**
	 * Make a connection to the URL.
	 * 
	 * @param url
	 *            The URL to connect.
	 * @return The data retrieved from the connection.
	 * @throws RGMAException
	 *             If unable to connect.
	 * @throws IOException
	 */
	private String connect(URL url) throws RGMAPermanentException, IOException {
		LOG.debug("Connect using " + m_method + " to: " + url);

		// Create a handle to this Http i/o
		URLConnection urlConnection = url.openConnection();

		// Check if POST has been configured
		if (m_method != HttpMethod.GET) {
			// POST is required, so check if the contents have been set
			if ((m_postMessage != null) && (m_postMessage.length() > 0)) {
				LOG.debug("About to POST data.");
				urlConnection.setDoOutput(true);
				urlConnection.setDoInput(true);

				// Contents have been set, so send this message
				OutputStreamWriter outputWriter = new OutputStreamWriter(urlConnection.getOutputStream());

				// Write the body
				outputWriter.write(m_postMessage);
				outputWriter.flush();
				outputWriter.close();
				LOG.debug("POST succeeded.");
			} else {
				LOG.error("POST body contents were not set.");

				// Body has not been set, so throw an RGMAException
				throw new RGMAPermanentException("Body not set for POST request");
			}
		}

		InputStreamReader inputReader = new InputStreamReader(urlConnection.getInputStream());
		BufferedReader bufferReader = new BufferedReader(inputReader);
		String input = null;

		// Initialise a StringBuffer to contain the response
		StringBuffer buffer = new StringBuffer(100);

		// Read-in the response from the Http Servlet
		while ((input = bufferReader.readLine()) != null) {
			// Append the line to the response message
			buffer.append(input + "\n");
		}

		bufferReader.close();
		inputReader.close();

		if (LOG.isDebugEnabled()) {
			LOG.debug("connect(URL) returned: " + buffer);
		}

		return buffer.toString();
	}

	/**
	 * Adds parameters to Http message, and sets the url protocol.
	 * 
	 * @param operation
	 *            A string defining the servlet operation to connect to.
	 * @return URL A URL containing the parameters and setting https if nesc. paths appended.
	 * @throws RGMAException
	 */
	private URL getConnectionUrl(String operation) throws RGMAPermanentException {
		String urlString = m_url + "/" + operation;

		// Check if any parameters have been set
		if (m_encodedParams.length() > 0) {
			if ((m_method == HttpMethod.GET) || (m_method == HttpMethod.POST_WITH_ENCODED_PARAMS)) {
				// Append the parameter to the end of the URL
				urlString = urlString + "?" + m_encodedParams.toString();
			} else {
				// Put the parameters in the body
				m_postMessage = m_encodedParams.toString();
			}
		}

		try {
			return new URL(urlString);
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException("Invalid URL for servlet connection: " + urlString);
		}
	}
}
