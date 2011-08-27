/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2008.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.glite.rgma.server.system.RGMAPermanentException;

public class ClientAccessConfigurator {

	private static final Object s_instanceLock = new Object();

	private static ClientAccessConfigurator s_clientAccessConfigurator;

	public static void dropInstance() {
		synchronized (s_instanceLock) {
			if (s_clientAccessConfigurator != null) {
				s_clientAccessConfigurator.shutdown();
				s_clientAccessConfigurator = null;
			}
		}
	}

	public static ClientAccessConfigurator getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_clientAccessConfigurator == null) {
				s_clientAccessConfigurator = new ClientAccessConfigurator();
			}
			return s_clientAccessConfigurator;
		}
	}

	private final Timer m_Timer;

	private long m_timeStamp;

	private static int s_CheckIntervalSecs;

	private static File s_PatternsFile;

	private static final Logger s_controlLogger = Logger.getLogger(ServerConstants.CONTROL_LOGGER);

	private ClientAccessConfigurator() throws RGMAPermanentException {
		final ServerConfig sc = ServerConfig.getInstance();
		s_PatternsFile = new File(sc.getString(ServerConstants.SERVER_ALLOWED_CLIENT_HOSTNAME_PATTERNS_FILE));
		s_CheckIntervalSecs = sc.getInt(ServerConstants.SERVER_ALLOWED_CLIENT_CHECK_INTERVAL_SECS);
		m_Timer = new Timer(true);
		checkAccessFile();
		m_Timer.schedule(new TimerTask() {
			@Override
			public final void run() {
				checkAccessFile();
			}
		}, s_CheckIntervalSecs * 1000, s_CheckIntervalSecs * 1000);
		s_controlLogger.info("ClientAccessConfigurator started");
	}

	private synchronized void checkAccessFile() {
		if (!s_PatternsFile.exists()) {
			s_controlLogger.error(s_PatternsFile + " file does not exist. It is needed by the ClientAccessConfigurator");
			return;
		}
		final long timeStamp = s_PatternsFile.lastModified();
		if (m_timeStamp != timeStamp) {
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(s_PatternsFile));
				String line = br.readLine();
				final List<String> suffices = new ArrayList<String>();
				while (line != null) {
					final String suffix = line.trim().toUpperCase();
					if (!suffix.startsWith("#")) {
						final List<String> deletes = new ArrayList<String>();
						for (final String s : suffices) {
							if (s.endsWith(suffix)) {
								s_controlLogger.warn(s + " is superseded by " + suffix + " in " + s_PatternsFile);
								deletes.add(s);
							} else if (suffix.endsWith(s)) {
								s_controlLogger.warn(suffix + " is superseded by " + s + " in " + s_PatternsFile);
								deletes.add(suffix);
							}
						}
						suffices.add(suffix);
						/*
						 * Only remove one element at a time otherwise duplicates will both be removed
						 */
						for (final String delete : deletes) {
							suffices.remove(delete);
						}
					}
					line = br.readLine();
				}
				m_timeStamp = timeStamp;
				Service.setClientPatterns(suffices);
			} catch (final FileNotFoundException e) {
				s_controlLogger.error(s_PatternsFile + " file does not exist. It is needed by the ClientAccessConfigurator");
			} catch (final IOException e) {
				s_controlLogger.error(s_PatternsFile + " has a read error. It is needed by the ClientAccessConfigurator");
			}
		}
	}

	private void shutdown() {
		m_Timer.cancel();
		s_controlLogger.info("ClientAccessConfigurator shutdown");
	}
}
