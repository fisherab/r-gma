package org.glite.rgma.server.servlets;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocketFactory;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.security.trustmanager.ContextWrapper;
import org.glite.security.util.CaseInsensitiveProperties;

public class RGMAContextWrapper {

	private static Object s_instanceLock = new Object();

	private static Logger LOG = Logger.getLogger("rgma.servlets");

	private static RGMAContextWrapper s_sender;

	public static void dropInstance() {
		synchronized (s_instanceLock) {
			if (s_sender != null) {
				s_sender = null;
			}
		}
	}

	/**
	 * Get the singleton object.
	 * 
	 * @throws RGMAPermanentException
	 */
	public static RGMAContextWrapper getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_sender == null) {
				s_sender = new RGMAContextWrapper();
			}
			return s_sender;
		}
	}

	private ContextWrapper m_contextSSL;

	private RGMAContextWrapper() throws RGMAPermanentException {
		ServerConfig serverConfig = ServerConfig.getInstance();
		String proxyfile = serverConfig.getString(ServerConstants.SERVLETCONNECTION_X509_USER_PROXY);
		String cadir = serverConfig.getString(ServerConstants.SERVLETCONNECTION_X509_CERT_DIR);
		LOG.debug("X509_CERT_DIR: " + cadir);
		CaseInsensitiveProperties trustproperties = new CaseInsensitiveProperties();
		if (new File(proxyfile).exists()) {
			trustproperties.setProperty("gridProxyFile", proxyfile);
			LOG.debug("X509_USER_PROXY: " + proxyfile + " will be used by context wrapper");
		} else {
			trustproperties.setProperty("sslCertFile", "/etc/grid-security/hostcert.pem");
			trustproperties.setProperty("sslKey", "/etc/grid-security/hostkey.pem");
			LOG.debug("'/etc/grid-security/' hostkey.pem and hostcert.pem will be used by context wrapper");
		}
		trustproperties.setProperty("sslCaFiles", cadir + File.separator + "*.0");
		trustproperties.setProperty("crlFiles", cadir + File.separator + "*.r0");
		trustproperties.setProperty("credentialsUpdateInterval", "1h");
		try {
			m_contextSSL = new ContextWrapper(trustproperties);
		} catch (IOException e) {
			throw new RGMAPermanentException(e);
		} catch (GeneralSecurityException e) {
			throw new RGMAPermanentException(e);
		}
	}

	public SSLContext getContext() {
		return m_contextSSL.getContext();
	}

	public SSLSocketFactory getSocketFactory() throws RGMAPermanentException {
		try {
			return m_contextSSL.getSocketFactory();
		} catch (SSLException e) {
			throw new RGMAPermanentException(e);
		}
	}

}
