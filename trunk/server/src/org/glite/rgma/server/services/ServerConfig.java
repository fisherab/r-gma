/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * Properties object used to configure an R-GMA Server
 * 
 * @author $author$
 * @version $Revision: 1.17 $
 */
@SuppressWarnings("serial")
public class ServerConfig extends Properties {

	static ServerConfig s_config;

	private ServerConfig() throws RGMAPermanentException {
		InputStream is = null;
		String rgmaHome = System.getProperty(ServerConstants.RGMA_HOME_PROPERTY);
		if (rgmaHome == null) {
			throw new RGMAPermanentException("System property RGMA_HOME is not set");
		}
		try {
			is = new FileInputStream(rgmaHome + ServerConstants.SERVER_CONFIG_LOCATION);
			load(is);
		} catch (IOException e) {
			throw new RGMAPermanentException("ServerConfiguration could not be loaded from: " + rgmaHome + ServerConstants.SERVER_CONFIG_LOCATION + " "
					+ e.getMessage());
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					// No action
				}
			}
		}
	}

	/**
	 * Get an instance of the server configuration.
	 * 
	 * @throws RGMAConfiguration
	 *             exception if the loading of the server configuration file fails
	 * @return the current server configuration
	 */
	public static synchronized ServerConfig getInstance() throws RGMAPermanentException {
		if (s_config == null) {
			s_config = new ServerConfig();
		}
		return s_config;
	}

	public String getString(String name) throws RGMAPermanentException {
		String value = getProperty(name);
		if (value != null) {
			return value.trim();
		} else {
			throw new RGMAPermanentException("Required parameter not found: " + name);
		}
	}

	public boolean getBoolean(String name) throws RGMAPermanentException {
		String str = getProperty(name);
		if (str == null) {
			throw new RGMAPermanentException("Required parameter " + name + " not found");
		} else {
			str = str.trim();
			if (str.equalsIgnoreCase("TRUE")) {
				return true;
			} else if (str.equalsIgnoreCase("FALSE")) {
				return false;
			} else {
				throw new RGMAPermanentException("Boolean parameter " + name + " had invalid value: " + str);
			}
		}
	}

	public long getLong(String name) throws RGMAPermanentException {
		String str = getProperty(name);
		if (str == null) {
			throw new RGMAPermanentException("Required parameter " + name + " not found");
		} else {
			try {
				return Long.parseLong(str.trim());
			} catch (NumberFormatException e) {
				throw new RGMAPermanentException("Long parameter " + name + " had invalid value: " + str);
			}
		}
	}

	public int getInt(String name) throws RGMAPermanentException {
		String str = getProperty(name);
		if (str == null) {
			throw new RGMAPermanentException("Required parameter " + name + " not found");
		} else {
			try {
				return Integer.parseInt(str.trim());
			} catch (NumberFormatException e) {
				throw new RGMAPermanentException("Integer parameter " + name + " had invalid value: " + str);
			}
		}
	}
}
