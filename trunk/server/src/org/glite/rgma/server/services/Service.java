/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glite.rgma.server.services.streaming.StreamingReceiver;
import org.glite.rgma.server.services.streaming.StreamingSender;
import org.glite.rgma.server.services.tasks.Task;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.servlets.RGMAServlet;
import org.glite.rgma.server.servlets.ServletConnection;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.SystemContextInterface;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;

/**
 * Base class for services
 */
public abstract class Service {

	protected static final Logger s_controlLogger = Logger.getLogger(ServerConstants.CONTROL_LOGGER);

	protected static final Logger s_securityLogger = Logger.getLogger(ServerConstants.SECURITY_LOGGER);

	private static final long MB = 1024 * 1024;

	protected static TaskManager s_taskManager;

	private static String s_rgmaHome;

	private static ServerConfig s_serverConfig;

	private static String s_serviceVersion;

	protected String m_serviceName;

	private static final long s_server_start_time_millis = System.currentTimeMillis();

	private String m_status = null;

	private String m_URLString;

	protected Logger m_logger;

	private static MemoryPoolMXBean s_mpbean;

	private static GarbageCollectorMXBean s_gcbean;

	private static long s_maxHeap;

	private static String s_mmname;

	private static String s_hostname;

	private URL m_URL;

	private static StreamingSender s_streamingSender;

	private static StreamingReceiver s_streamingReceiver;

	private static final Object s_instanceLock = new Object();

	// Valid table or column name pattern: ( <LETTER> )+ ( <DIGIT> | <LETTER> |<SPECIAL_CHARS>)*
	private static final Pattern s_tableOrColumnNamePattern = Pattern.compile("[a-zA-Z][0-9a-zA-Z\\$_]*");

	private static List<String> s_clientSuffices = new ArrayList<String>();

	private static int s_maximumRequestCount;

	private static final Map<String, Set<String>> s_hostsForVDB = new HashMap<String, Set<String>>();

	/**
	 * Creates a new Service object.
	 * 
	 * @param rgmaHome
	 *            DOCUMENT ME!
	 * @throws RGMAPermanentException
	 */
	public Service(String serviceName) throws RGMAPermanentException {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		m_logger = Logger.getRootLogger(); // This logger will normally be replaced
		try {
			m_serviceName = serviceName;

			// Check hostname
			ServerConfig serverConfig = ServerConfig.getInstance();
			String hostname = serverConfig.getString(ServerConstants.SERVER_HOSTNAME);
			try {
				InetAddress.getByName(hostname).getCanonicalHostName();
			} catch (UnknownHostException e) {
				throw new RGMAPermanentException("Hostname not recognised: " + hostname);
			}
			m_URLString = ServerConstants.SERVICE_SECURE_URL_PREFIX + hostname + ":" + serverConfig.getString(ServerConstants.SERVER_PORT) + "/"
					+ ServerConstants.WEB_APPLICATION_NAME + "/" + serviceName;
			try {
				m_URL = new URL(m_URLString);
			} catch (MalformedURLException e) {
				throw new RGMAPermanentException("Internally generated URL is malformed", e);
			}

			synchronized (s_instanceLock) {
				if (s_mpbean == null) {
					s_hostname = hostname;

					s_rgmaHome = System.getProperty(ServerConstants.RGMA_HOME_PROPERTY);
					s_taskManager = TaskManager.getInstance();
					s_serverConfig = serverConfig;
					BufferedReader br = null;
					try {
						String loc = serverConfig.getProperty(ServerConstants.SERVER_VERSION_FILE_LOCATION);
						if (loc != null) {
							br = new BufferedReader(new FileReader(loc));
							String line = br.readLine();
							while (line != null) {
								if (!line.trim().equals("")) {
									if (s_serviceVersion == null) {
										s_serviceVersion = line.trim();
									} else {
										throw new RGMAPermanentException("Service version file: " + loc + " must have one line with the service version number");
									}
								}
								line = br.readLine();
							}
						} else {
							throw new RGMAPermanentException("Value of " + ServerConstants.SERVER_VERSION_FILE_LOCATION + " is not set");
						}
						if (s_serviceVersion == null) {
							throw new RGMAPermanentException("Service version file: " + loc + " must have one line with the service version number");
						}
						m_logger.debug("Version of RGMA Server is " + s_serviceVersion);
					} catch (IOException e) {
						throw new RGMAPermanentException("Could not find file: " + e.getMessage());
					} finally {
						if (br != null) {
							try {
								br.close();
							} catch (IOException e) {
								// Ignore
							}
						}
					}

					Set<String> poolNames = new HashSet<String>();
					int i = 1;
					while (true) {
						String poolName = serverConfig.getProperty(ServerConstants.SERVER_POOL_TO_WATCH + i);
						if (poolName == null) {
							break;
						}
						poolNames.add(poolName);
						i++;
					}
					long maxHeadRoom = serverConfig.getLong(ServerConstants.SERVER_MAX_HEAD_ROOM);

					List<MemoryPoolMXBean> mpbeans = ManagementFactory.getMemoryPoolMXBeans();

					for (MemoryPoolMXBean mpbean : mpbeans) {
						if (poolNames.contains(mpbean.getName())) {
							if (mpbean.isCollectionUsageThresholdSupported()) {
								s_maxHeap = mpbean.getUsage().getMax();
								float maxFull = s_maxHeap - maxHeadRoom;
								mpbean.setCollectionUsageThreshold((long) maxFull);
								if (s_mpbean == null) {
									s_mpbean = mpbean;
								} else {
									StringBuilder s = new StringBuilder("More than one memory pool matches.");
									addPoolInfo(s, poolNames, mpbeans);
									throw new RGMAPermanentException(s.toString());
								}
							}
						}
					}
					if (s_mpbean == null) {
						StringBuilder s = new StringBuilder("No memory pool matches.");
						addPoolInfo(s, poolNames, mpbeans);
						throw new RGMAPermanentException(s.toString());
					}

					String[] mmnames = s_mpbean.getMemoryManagerNames();
					if (mmnames.length != 1) {
						StringBuilder s = new StringBuilder("Memory pool " + s_mpbean.getName() + " does not have exactly one memory manager.");
						addPoolInfo(s, poolNames, mpbeans);
						throw new RGMAPermanentException(s.toString());
					}
					s_mmname = mmnames[0];
					for (GarbageCollectorMXBean gcbean : ManagementFactory.getGarbageCollectorMXBeans()) {
						if (gcbean.getName().equals(s_mmname)) {
							s_gcbean = gcbean;
						}
					}
					s_streamingSender = StreamingSender.getInstance();
					s_streamingReceiver = StreamingReceiver.getInstance();
					ServletConnection.init();
				}
				ClientAccessConfigurator.getInstance();
				s_maximumRequestCount = serverConfig.getInt(ServerConstants.SERVER_MAXIMUM_REQUEST_COUNT);
			}
		} catch (RGMAPermanentException e) {
			m_logger.fatal(e.getMessage());
			throw e;
		}
	}

	private void addPoolInfo(StringBuilder s, Set<String> poolNames, List<MemoryPoolMXBean> mpbeans) {
		s.append(" Java VM is " + System.getProperty("java.version", "unknown"));
		s.append(" from " + System.getProperty("java.vendor", "unknown vendor"));
		s.append(". Requested pools were:");
		for (String poolName : poolNames) {
			s.append(" '" + poolName + "'");
		}
		s.append(". available pools are:");
		for (MemoryPoolMXBean mpbean : mpbeans) {
			s.append(" '" + mpbean.getName() + "'");
		}
	}

	public static void checkMemoryLow() throws RGMATemporaryException {
		if (s_mpbean.isCollectionUsageThresholdExceeded()) {
			System.gc();
			throw new RGMATemporaryException("Memory is low on the R-GMA server");
		}
	}

	public static void checkBusy() throws RGMATemporaryException {
		checkMemoryLow();
		int n;
		if ((n = RGMAServlet.getCurrentRequestCount()) > s_maximumRequestCount) {
			throw new RGMATemporaryException("Server is busy dealing with " + n + " requests. Maximum allowed is " + s_maximumRequestCount);
		}
	}

	public String getURLString() {
		return m_URLString;
	}

	public URL getURL() {
		return m_URL;
	}

	protected void setOffline(String msg) {
		m_status = msg;
	}

	protected void setOnline() {
		m_status = null;
	}

	protected void checkOnline() throws RGMAPermanentException {
		if (m_status != null) {
			throw new RGMAPermanentException("Service " + m_serviceName + " unavailable: " + m_status);
		}
	}

	/**
	 * get the RGMA_HOME directory
	 * 
	 * @return the path to the RGMA_HOME directory
	 */
	protected String getRgmaHome() {
		return s_rgmaHome;
	}

	protected String getHostname() {
		return s_hostname;
	}

	protected ServerConfig getServerConfig() {
		return s_serverConfig;
	}

	/**
	 * @param context
	 *            context of the request
	 * @param name
	 *            the name of the property to get
	 * @param param
	 *            the extra property parameter
	 * @return the value of the property parameter
	 * @throws RGMAPermanentException
	 * @throws
	 * @throws RGMAException
	 * @throws RGMAPermanentException
	 */
	public abstract String getProperty(String name, String param) throws UnknownResourceException, RGMAPermanentException;

	protected String getServiceProperty(String name, String param) throws RGMAPermanentException {
		if (name.equalsIgnoreCase(ServerConstants.SERVICE_STATUS_DETAILS)) {
			return getServiceStatusDetails(true);

		} else if (name.equalsIgnoreCase(ServerConstants.SERVICE_STATUS)) {
			return getServiceStatusDetails(false);

		} else if (name.equalsIgnoreCase(ServerConstants.STREAMING_STATUS)) {
			return getStreamingStatus();

		} else if (name.equalsIgnoreCase(ServerConstants.TASKMANAGER_SUMMARY)) {
			return getTaskManagerSummary();

		} else if (name.equalsIgnoreCase(ServerConstants.PROPERTY_LOGGING_LEVEL)) {
			String returnValue;
			if (param == null || param.equals("")) {
				returnValue = getLoggingLevel();
			} else {
				Logger logger = LogManager.getLogger(param);
				if (logger == null) {
					returnValue = "Unknown class: " + param;
				} else {
					Level level = logger.getEffectiveLevel();
					StringBuilder b = new StringBuilder();
					b.append("<LoggingConfiguration>\n");
					b.append("<Class Name=\"").append(param);
					b.append("\" Level=\"");

					if (level == null) {
						b.append(ServerConstants.UNDEFINED_LOG_LEVEL + "\"/>\n");
					} else {
						b.append(level.toString()).append("\"/>\n");
					}

					b.append("</LoggingConfiguration>\n");
					returnValue = b.toString();
				}
			}
			return wrapData(returnValue);
		} else {
			throw new RGMAPermanentException("Unrecognised property name: " + name);
		}
	}

	protected final String wrapData(String data) {
		StringBuilder b = new StringBuilder();
		b.append("<Service>\n");
		b.append(data);
		b.append("</Service>\n");
		return b.toString();
	}

	private String getServiceStatusDetails(boolean all) {
		StringBuilder b = new StringBuilder();
		b.append("<Service ");
		String code = "OK";
		String message = null;
		try {
			checkOnline();
		} catch (RGMAPermanentException e) {
			code = "OFFLINE";
			message = m_status;
		}

		if (code == "OK") {
			try {
				checkBusy();
				s_taskManager.checkBusy();
			} catch (RGMATemporaryException e) {
				code = "BUSY";
				message = e.getMessage();
			}
		}

		b.append("StatusCode=\"").append(code).append("\"\n");
		b.append("StatusMessage=\"").append(message).append("\"\n");
		b.append("ServiceVersion=\"").append(s_serviceVersion).append("\"\n");
		b.append("ServiceTimeMillis=\"").append(System.currentTimeMillis()).append("\"\n");
		b.append("ServiceStartTimeMillis=\"").append(s_server_start_time_millis).append("\"\n");
		b.append("CurrentRequestCount=\"").append(RGMAServlet.getCurrentRequestCount()).append("\"\n");
		b.append("HighestRequestCount=\"").append(RGMAServlet.getHighestRequestCount()).append("\"\n");
		b.append("MaximumRequestCount=\"").append(s_maximumRequestCount).append("\"\n");

		if (all) {
			Runtime runtime = Runtime.getRuntime();
			b.append("JVMVersion=\"").append(System.getProperty("java.version", "")).append("\"\n");
			b.append("JVMMaxMemoryMB=\"").append(runtime.maxMemory() / MB).append("\"\n");
			b.append("JVMGCHeapUsePercentage=\"").append(String.format("%6.2f", s_mpbean.getCollectionUsage().getUsed() * 100. / s_maxHeap).trim()).append(
					"\"\n");
			b.append("JVMHeapUsePercentage=\"").append(String.format("%6.2f", s_mpbean.getUsage().getUsed() * 100. / s_maxHeap).trim()).append("\"\n");
			b.append("JVMGCCount=\"").append(s_gcbean.getCollectionCount()).append("\"\n");
			b.append("JVMMemoryManagers=\"").append(s_mmname).append("\"\n");
			b.append("ServiceURL=\"").append(m_URLString).append("\"\n");
		}

		b.append("/>");
		return b.toString();
	}

	private String getStreamingStatus() {
		StringBuilder b = new StringBuilder();
		b.append("<Streaming>\n");

		b.append("<StreamingReceiver");
		Map<String, String> receiverStatus = s_streamingReceiver.statusInfo();
		for (String key : receiverStatus.keySet()) {
			b.append(" ").append(key).append("=\"").append(receiverStatus.get(key)).append("\"");
		}
		b.append(">\n");
		List<Map<String, String>> replies = s_streamingReceiver.replyInfo(100);
		for (Map<String, String> map : replies) {
			b.append("<RunningReply");
			for (String key : map.keySet()) {
				b.append(" ").append(key).append("=\"").append(map.get(key)).append("\"");
			}
			b.append("/>\n");
		}

		b.append("</StreamingReceiver>\n");
		b.append("<StreamingSender");
		Map<String, String> senderStatus = s_streamingSender.statusInfo();
		for (String key : senderStatus.keySet()) {
			b.append(" ").append(key).append("=\"").append(senderStatus.get(key)).append("\"");
		}
		b.append(">\n");
		List<Map<String, String>> connection = s_streamingSender.connectionInfo();
		for (Map<String, String> map : connection) {
			b.append("<Connection");
			for (String key : map.keySet()) {
				b.append(" ").append(key).append("=\"").append(map.get(key)).append("\"");
			}
			b.append("/>\n");
		}
		b.append("</StreamingSender>\n");
		b.append("</Streaming>\n");
		return b.toString();
	}

	private String getTaskManagerSummary() {
		StringBuilder b = new StringBuilder();
		b.append("<TaskManager\n");
		Map<String, String> status = s_taskManager.statusInfo();
		// extracts taskmanager servicestatusinfo from map
		for (String key : status.keySet()) {
			b.append(key).append("=\"").append(status.get(key)).append("\" ");
		}
		b.append(">\n");

		Map<String, String> invocatorStatus = s_taskManager.invocatorStatusInfo();
		for (String key : invocatorStatus.keySet()) {
			b.append("<TaskInvocator\n");
			b.append("ID=\"").append(key).append("\" ");
			b.append("GoodOnly=\"").append(invocatorStatus.get(key)).append("\">");
			b.append("</TaskInvocator>\n");
		}

		b.append(getGoodKeys());
		b.append("<QueuedAndRunningTasks>\n");
		b.append(getTasksPerKey());
		b.append(getNotGoodKeys());
		b.append("<RunningTasks>\n");
		b.append(getTaskDetails(s_taskManager.getRunningTasks()));
		b.append("</RunningTasks>\n");
		b.append("<QueuedTasks>\n");
		b.append(getTaskDetails(s_taskManager.getQueuedTasks()));
		b.append("</QueuedTasks>\n");
		b.append("</QueuedAndRunningTasks>\n");
		b.append("</TaskManager>\n");
		return b.toString();
	}

	private String getTaskDetails(List<Task> list) {
		StringBuilder b = new StringBuilder();
		int n = 0;
		for (Task task : list) {
			b.append("<Task ");
			Map<String, String> status = task.statusInfo();
			for (String key : status.keySet()) {
				b.append(key).append("=\"").append(status.get(key)).append("\" ");
			}
			b.append("/>\n");
			if (++n >= 100) {
				break;
			}
		}
		return b.toString();
	}

	private String getTasksPerKey() {
		StringBuilder b = new StringBuilder();
		b.append("<TasksPerKey>\n");
		Map<String, Integer> tasks = s_taskManager.getTasksPerKey();
		for (String key : tasks.keySet()) {
			b.append("<Task Key=\"").append(key).append("\" Count=\"").append(tasks.get(key)).append("\"/>\n");
		}
		b.append("</TasksPerKey>\n");
		return b.toString();
	}

	private String getGoodKeys() {
		StringBuilder b = new StringBuilder();
		List<String> keys = s_taskManager.getCopyGoodKeys();
		b.append("<GoodKeys>\n");
		for (String str : keys) {
			b.append("<Key>" + str + "</Key>\n");
		}
		b.append("</GoodKeys>\n");
		return b.toString();
	}

	private String getNotGoodKeys() {
		StringBuilder b = new StringBuilder();
		List<String> keys = s_taskManager.getNotGoodKeys();
		b.append("<NotGoodKeys>\n");
		for (String str : keys) {
			b.append("<key>" + str + "</key>\n");
		}
		b.append("</NotGoodKeys>\n");
		return b.toString();
	}

	/**
	 * @return Returns an XML list of all classes with their associated logging level.
	 */
	@SuppressWarnings("unchecked")
	private final String getLoggingLevel() {

		Enumeration<Category> logEnum = LogManager.getCurrentLoggers();
		TreeMap<String, Level> map = new TreeMap<String, Level>();

		while (logEnum.hasMoreElements()) {
			Category category = logEnum.nextElement();
			map.put(category.getName(), category.getEffectiveLevel());
		}

		StringBuffer b = new StringBuffer();
		b.append("<LoggingConfiguration>\n");

		Iterator<String> it = map.keySet().iterator();

		while (it.hasNext()) {
			String className = it.next();
			Level level = map.get(className);
			b.append("<Class Name=\"").append(className);
			b.append("\" Level=\"");

			if (level == null) {
				b.append(ServerConstants.UNDEFINED_LOG_LEVEL + "\"/>\n");
			} else {
				b.append(level.toString()).append("\"/>\n");
			}
		}

		b.append("</LoggingConfiguration>\n");
		return b.toString();
	}

	/**
	 * Return the version of this service
	 * 
	 * @return the version of the service
	 */
	public static String getVersion() {
		return s_serviceVersion;
	}

	public final void ping() throws RGMAPermanentException {
		checkOnline();
	}

	public static void checkUserInputValid(String userInput) throws RGMAPermanentException {
		Matcher m = s_tableOrColumnNamePattern.matcher(userInput);
		if (!m.matches()) {
			s_securityLogger.warn("Invalid format for identifier: '" + userInput + "'");
			throw new RGMAPermanentException("Invalid format for identifier: '" + userInput + "'");
		}
	}

	public static void setClientPatterns(List<String> sufficesUpper) {
		if (sufficesUpper.size() == 0) {
			s_controlLogger.warn("All client access is denied - is this intended?");
		} else if (sufficesUpper.get(0).length() == 0) {
			s_controlLogger.warn("All client access is allowed - is this intended?");
		} else if (s_controlLogger.isInfoEnabled()) {
			StringBuffer s = new StringBuffer("Clients with hostnames with the following endings are permitted access:");
			for (String suffix : sufficesUpper) {
				s.append(" '").append(suffix).append('\'');
			}
			s_controlLogger.info(s);
		}
		s_clientSuffices = sufficesUpper;
	}

	protected static void checkUserContext(UserContext context) throws RGMAPermanentException {
		String hostname = context.getHostName();
		for (String suffix : s_clientSuffices) {
			if (hostname.toUpperCase().endsWith(suffix)) {
				return;
			}
		}
		throw new RGMAPermanentException("Server " + s_hostname + " is not configured to accept user calls from this client: " + hostname);
	}

	public static void checkSystemContext(String vdbName, SystemContextInterface context) throws RGMAPermanentException {
		String vdbNameUpper = vdbName.toUpperCase();
		Set<String> vdbNamesUpper = s_hostsForVDB.get(vdbNameUpper);
		if (vdbNamesUpper != null && vdbNamesUpper.contains(context.getConnectingHostName())) {
			return;
		}
		throw new RGMAPermanentException("Server is not configured to accept system calls for " + vdbName + " from "
				+ context.getConnectingHostName().toLowerCase());
	}

	public static void setHostsForVDB(String vdbnameUpper, Set<String> hostnamesUpper) {
		s_hostsForVDB.put(vdbnameUpper, hostnamesUpper);
	};

	public enum Api {
		USER_API, SYSTEM_API
	}

	/** Only needed for testing purposes */
	public static void replaceSingletons() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_taskManager != null) {
				s_taskManager = TaskManager.getInstance();
			}
			if (s_streamingSender != null) {
				s_streamingSender = StreamingSender.getInstance();
			}
			if (s_streamingReceiver != null) {
				s_streamingReceiver = StreamingReceiver.getInstance();
			}
		}
	}
}