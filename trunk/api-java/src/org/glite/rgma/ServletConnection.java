/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.security.cert.CertificateException;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import org.glite.security.trustmanager.ContextWrapper;
import org.glite.security.util.CaseInsensitiveProperties;

/**
 * A ServletConnection Object is used to connect to a particular Http based Servlet. The URL is defined by the client
 * and Http GET or Http POST can be used. By default, GET is the default transport method although this can be changed
 * by using the setRequestMethodPost().
 */
class ServletConnection {

	private static enum Transport {
		POST, GET
	};

	private static String s_serverCommonURL;

	public static String getServerCommonURL() throws RGMAPermanentException {
		if (s_exception != null) {
			throw s_exception;
		}
		return s_serverCommonURL;
	}

	private static RGMAPermanentException s_exception;

	static {
		try {
			InputStream is = null;
			String rgmaHome = System.getProperty("RGMA_HOME");
			if (rgmaHome == null) {
				rgmaHome = System.getProperty("GLITE_LOCATION");
			}
			if (rgmaHome == null) {
				throw new RGMAPermanentException("System property RGMA_HOME is not set");
			}
			File f = new File(new File(new File(rgmaHome, "etc"), "rgma"), "rgma.conf");
			Properties props = new Properties();
			try {
				props.load(new FileInputStream(f));
			} catch (IOException e) {
				throw new RGMAPermanentException("R-GMA Client configuration could not be loaded from: " + f);
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (IOException e) {
						// No action
					}
				}
			}
			String hostname = props.getProperty("hostname");
			if (hostname == null) {
				throw new RGMAPermanentException("R-GMA Client configuration must specify \"hostname\"");
			}
			String port = props.getProperty("port");
			if (port == null) {
				throw new RGMAPermanentException("R-GMA Client configuration must specify \"port\"");
			}
			String prefix = props.getProperty("prefix");
			if (prefix == null) {
				prefix = "R-GMA";
			}
			s_serverCommonURL = "https://" + hostname + ":" + port + "/" + prefix + "/";

			try {
				new URL(s_serverCommonURL);
			} catch (MalformedURLException e1) {
				throw new RGMAPermanentException("String " + s_serverCommonURL
						+ " formed from hostname and port in R-GMA Client configuration is not a valid URL");
			}

			String trustfile = System.getProperty("TRUSTFILE");
			String proxyfile = System.getProperty("X509_USER_PROXY");
			String cadir = System.getProperty("X509_CERT_DIR");

			CaseInsensitiveProperties trustproperties = new CaseInsensitiveProperties();

			if (proxyfile != null) {
				trustproperties.setProperty("gridProxyFile", proxyfile);
				trustproperties.setProperty("credentialsUpdateInterval", "1h");
				if (cadir != null) {
					trustproperties.setProperty("sslCaFiles", cadir + File.separator + "*.0");
					trustproperties.setProperty("crlFiles", cadir + File.separator + "*.r0");
				}
			} else if (trustfile != null) {
				trustproperties.setProperty("sslConfigFile", trustfile);
			} else {
				throw new RGMAPermanentException("Neither TRUSTFILE nor X509_USER_PROXY is set");
			}

			try {
				ContextWrapper contextSSL = new ContextWrapper(trustproperties);
				SSLSocketFactory sslSocketFactory = contextSSL.getSocketFactory();
				HttpsURLConnection.setDefaultSSLSocketFactory(sslSocketFactory);
				HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
					public boolean verify(String urlHostname, SSLSession certHostname) {
						return true;
					}
				});
			} catch (CertificateException e) {
				throw new RGMAPermanentException("Client certificate error: " + e.getMessage());
			} catch (Exception e) {
				throw new RGMAPermanentException("Failed to set up secure connection: " + e.getMessage());
			}

		} catch (RGMAPermanentException e) {
			s_exception = e;
		}
	}

	/**
	 * Replaces special characters with hex representation as "%XX".
	 */
	static String encodeForPost(String value) {
		// % %25
		value = replaceSpecialChar(value, '%', "%25");

		// + %2B
		value = replaceSpecialChar(value, '+', "%2B");

		// & %26
		value = replaceSpecialChar(value, '&', "%26");

		// = %3D
		value = replaceSpecialChar(value, '=', "%3D");

		// : %3A
		value = replaceSpecialChar(value, ':', "%3A");

		// / %2F
		value = replaceSpecialChar(value, '/', "%2F");

		return value;
	}

	static String replaceSpecialChar(String value, char oldChar, String newCode) {
		StringBuilder out = new StringBuilder();
		int charIndex = value.indexOf(oldChar);
		int firstChar = 0;
		if (charIndex != -1) {
			while (charIndex != -1) {
				out.append(value.substring(firstChar, charIndex)).append(newCode);
				firstChar = charIndex + 1;
				charIndex = value.indexOf(oldChar, firstChar);
			}
			out.append(value.substring(firstChar, value.length()));
		} else {
			return value;
		}
		return out.toString();
	}

	/** URL to connect */
	protected String m_servletURL;

	/** Buffer to store all parameters used when creating the URL */
	protected StringBuilder m_encodedParams;

	/** HTTPS transport method to be used */
	private Transport m_transport;

	public ServletConnection(String servletName) throws RGMAPermanentException {
		if (s_exception != null) {
			throw s_exception;
		}
		m_encodedParams = new StringBuilder(50);
		m_transport = Transport.GET;
		m_servletURL = s_serverCommonURL + servletName;
	}

	public void addParameter(String name, boolean value) throws RGMAPermanentException {
		addParameter(name, value + "");
	}

	public void addParameter(String name, double value) throws RGMAPermanentException {
		addParameter(name, String.valueOf(value));
	}

	public void addParameter(String name, int value) throws RGMAPermanentException {
		addParameter(name, String.valueOf(value));
	}

	public void addParameter(String name, String value) throws RGMAPermanentException {
		if (value == null) {
			throw new RGMAPermanentException("Invalid parameter: " + name + " is null.");
		}

		String escapedValue = null;

		if (m_transport == Transport.GET) {
			try {
				escapedValue = URLEncoder.encode(value, "UTF-8");
			} catch (java.io.UnsupportedEncodingException e) {
				throw new RGMAPermanentException("Registry.sendCommand() failed to encode message");
			}
		} else {
			escapedValue = encodeForPost(value);
		}

		if (m_encodedParams.length() == 0) {
			m_encodedParams.append(name + "=" + escapedValue);
		} else {
			m_encodedParams.append("&" + name + "=" + escapedValue);
		}
	}

	public TupleSet sendCommand(String command) throws RGMAPermanentException, RGMATemporaryException, UnknownResourceException {
		String xml = connect(command);
		return XMLSAXConverter.convertXMLResponse(xml);
	}

	public TupleSet sendNonResourceCommand(String command) throws RGMAPermanentException, RGMATemporaryException {
		String xml = connect(command);
		return XMLSAXConverter.convertXMLResponseWithoutUnknownResource(xml);
	}

	public void setRequestMethodPost() {
		m_transport = Transport.POST;
	}

	String connect(String operation) throws RGMAPermanentException, RGMATemporaryException {
		try {
			String postMessage = null;
			String urlString = m_servletURL + "/" + operation;
			if (m_encodedParams.length() > 0) {
				if (m_transport == Transport.GET) {
					urlString = urlString + "?" + m_encodedParams.toString();
				} else {
					postMessage = m_encodedParams.toString();
				}
			}
			URLConnection urlConnection = new URL(urlString).openConnection();
			if (m_transport == Transport.POST) {
				if (postMessage != null) {
					urlConnection.setDoOutput(true);
					urlConnection.setDoInput(true);
					OutputStreamWriter outputWriter = new OutputStreamWriter(urlConnection.getOutputStream());
					outputWriter.write(postMessage);
					outputWriter.flush();
					outputWriter.close();
				} else {
					throw new RGMAPermanentException("Empty Body for POST request");
				}
			}
			InputStreamReader inputReader = new InputStreamReader(urlConnection.getInputStream());
			BufferedReader bufferReader = new BufferedReader(inputReader);
			String input = null;
			StringBuilder buffer = new StringBuilder(100);
			while ((input = bufferReader.readLine()) != null) {
				buffer.append(input + "\n");
			}
			bufferReader.close();
			inputReader.close();
			return buffer.toString();
		} catch (SSLException e) {
			throw new RGMAPermanentException(e.getMessage());
		} catch (IOException e) {
			throw new RGMATemporaryException(e.getMessage());
		}
	}
}
