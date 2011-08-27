/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2008.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.registry.RegistryService;
import org.glite.rgma.server.services.schema.SchemaService;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class VDBConfigurator {

	private final class Rule {
		public Rule(String credentials, String action) {
			m_credentials = credentials;
			m_action = action;
		}
		private String m_credentials;
		private String m_action;
	}

	private final class Host {
		public Host(String name, int port, boolean isMasterSchema, boolean isRegistry) {
			m_name = name;
			m_port = port;
			m_isMasterSchema = isMasterSchema;
			m_isRegistry = isRegistry;
		}

		private String m_name;
		private int m_port;
		private boolean m_isMasterSchema;
		private boolean m_isRegistry;
	}

	private class VDBParser extends DefaultHandler {
		public String m_name = "";
		public Collection<Host> m_hosts = new ArrayList<Host>();
		public Collection<Rule> m_rules = new ArrayList<Rule>();
		private String m_error;

		/**
		 * Process start tag
		 */
		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) {
			if (qName.equals("vdb")) {
				m_name = attributes.getValue("name").trim();
			} else if (qName.equals("host")) {
				String name = attributes.getValue("name");
				if (name == null) {
					m_error = "vdb has no name";
				} else {
					name = name.trim();
				}
				int port = 0;
				try {
					port = Integer.parseInt(attributes.getValue("port"));
				} catch (NumberFormatException e) {
					m_error = "Non integer port value";
				}
				boolean isMasterSchema = Boolean.parseBoolean(attributes.getValue("masterSchema"));
				boolean isRegistry = Boolean.parseBoolean(attributes.getValue("registry"));
				m_hosts.add(new Host(name, port, isMasterSchema, isRegistry));
			} else if (qName.equals("rule")) {
				String credentials = attributes.getValue("credentials");
				if (credentials == null) {
					credentials = "";
				} else {
					credentials = credentials.trim();
				}
				String predicate = attributes.getValue("predicate");
				if (predicate == null) {
					predicate = "";
				} else {
					predicate = predicate.trim();
				}
				String action = attributes.getValue("action");
				if (action == null) {
					m_error = "rule has no action specified";
				} else {
					action = action.trim();
				}
				m_rules.add(new Rule(credentials, action));
			}
		}

		private void check(File file) throws RGMAPermanentException {
			if (m_error != null) {
				throw new RGMAPermanentException(m_error + " in vdb configuation file " + file);
			}
			if (!(m_name + ".xml").equalsIgnoreCase(file.getName())) {
				throw new RGMAPermanentException("File name " + file.getName() + " must be lower case of internal name with '.xml' extension");
			}
			List<String> schemas = new ArrayList<String>();
			for (Host host : m_hosts) {
				String name = host.m_name;
				if (host.m_isMasterSchema) {
					schemas.add(name);
				}
			}
			if (schemas.size() != 1) {
				StringBuffer s = new StringBuffer("There must be exactly one machine configured as master server for " + m_name + ". You have");
				for (String schema : schemas) {
					s.append(" " + schema);
				}
				throw new RGMAPermanentException(s.toString());
			}
		}
	}

	private static final Object s_instanceLock = new Object();

	private static VDBConfigurator s_VDBConfigurator;

	private static final Pattern s_VDBFileNamePattern = Pattern.compile("[a-z][0-9a-z\\$_]*\\.xml");

	public static VDBConfigurator getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_VDBConfigurator == null) {
				s_VDBConfigurator = new VDBConfigurator();
			}
			return s_VDBConfigurator;
		}
	}

	public static void dropInstance() {
		synchronized (s_instanceLock) {
			if (s_VDBConfigurator != null) {
				s_VDBConfigurator.shutdown();
				s_VDBConfigurator = null;
			}
		}
	}

	private Timer m_Timer;

	private File m_dir;

	private final Map<File, Long> m_files = new HashMap<File, Long>();

	private RegistryService m_registryService;

	private SchemaService m_schemaService;

	private final Map<String, RegistryCreateRequest> m_registryCreateRequests = new HashMap<String, RegistryCreateRequest>();

	private final Map<String, SchemaCreateRequest> m_schemaCreateRequests = new HashMap<String, SchemaCreateRequest>();

	private String m_hostname;

	private String m_VDBConfigurationDirectory;

	private int m_VDBConfigurationCheckIntervalSecs;

	private int m_port;

	private static final Logger s_controlLogger = Logger.getLogger(ServerConstants.CONTROL_LOGGER);

	private void shutdown() {
		m_Timer.cancel();
		s_controlLogger.info("VDBConfigurator shutdown");
	}

	private VDBConfigurator() throws RGMAPermanentException {
		ServerConfig sc = ServerConfig.getInstance();
		m_VDBConfigurationDirectory = sc.getString(ServerConstants.VDB_CONFIGURATION_DIRECTORY);
		m_VDBConfigurationCheckIntervalSecs = sc.getInt(ServerConstants.VDB_CONFIGURATION_CHECK_INTERVAL_SECS);
		m_hostname = sc.getString(ServerConstants.SERVER_HOSTNAME);
		m_port = sc.getInt(ServerConstants.SERVER_PORT);
		m_dir = new File(m_VDBConfigurationDirectory);
		m_Timer = new Timer(true);
		m_Timer.schedule(new TimerTask() {
			@Override
			public final void run() {
				checkConfigurations();
			}
		}, m_VDBConfigurationCheckIntervalSecs * 1000, m_VDBConfigurationCheckIntervalSecs * 1000);
		s_controlLogger.info("VDBConfigurator started");
	}

	public void registerSchemaService(SchemaService s) throws RGMAPermanentException {
		synchronized (this) {
			m_schemaService = s;
			for (SchemaCreateRequest req : m_schemaCreateRequests.values()) {
				req.transmit();
			}
			checkConfigurations();
		}
	}

	public void registerRegistryService(RegistryService r) throws RGMAPermanentException {
		synchronized (this) {
			m_registryService = r;
			for (RegistryCreateRequest req : m_registryCreateRequests.values()) {
				req.transmit();
			}
			checkConfigurations();
		}
	}

	private void checkConfigurations() {
		synchronized (this) {
			if (!m_dir.isDirectory()) {
				s_controlLogger.error(m_dir + " directory does not exist. It is needed by the VDBConfigurator");
				return;
			}
			/* Select .xml files */
			File[] contents = m_dir.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String fname) {
					return s_VDBFileNamePattern.matcher(fname).matches();
				}
			});
			if (contents.length == 0) {
				s_controlLogger.warn(m_dir + " directory is empty. It is needed by the VDBConfigurator to set up VDBs");
			}
			/*
			 * Find new, changed and removed files - note that if an exception is thrown the m_files is not changed
			 */
			for (File file : contents) {
				try {
					long timeStamp = file.lastModified();
					s_controlLogger.debug("VDBConfigurator reading " + file);
					if (m_files.containsKey(file)) {
						if (m_files.get(file) != timeStamp) {
							inform(file, Transition.CHANGED);
							m_files.put(file, timeStamp);
						}
					} else {
						inform(file, Transition.NEW);
						m_files.put(file, timeStamp);
					}
				} catch (RGMAPermanentException e) {
					s_controlLogger.error("RGMAPermanentException in VDBConfigurator processing " + file + ": " + e);
				}
			}
			Set<File> oldKeys = new HashSet<File>(m_files.keySet());
			oldKeys.removeAll(Arrays.asList(contents));
			for (File file : oldKeys) {
				try {
					inform(file, Transition.REMOVED);
					m_files.remove(file);
				} catch (RGMAPermanentException e) {
					s_controlLogger.error("RGMAPermanentException in VDBConfigurator processing " + file + ": " + e);
				}
			}
		}
	}

	private enum Transition {
		CHANGED, NEW, REMOVED
	};

	private void inform(File f, Transition t) throws RGMAPermanentException {
		if (t != Transition.REMOVED) {
			VDBParser parser = new VDBParser();
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			XMLReader xmlReader;
			try {
				xmlReader = saxFactory.newSAXParser().getXMLReader();
				xmlReader.setContentHandler(parser);
				xmlReader.setErrorHandler(parser);
				xmlReader.parse(new InputSource(new FileReader(f)));
			} catch (SAXException e) {
				throw new RGMAPermanentException(e);
			} catch (ParserConfigurationException e) {
				throw new RGMAPermanentException(e);
			} catch (FileNotFoundException e) {
				throw new RGMAPermanentException(e);
			} catch (IOException e) {
				throw new RGMAPermanentException(e);
			}
			parser.check(f);

			String vdbname = parser.m_name;
			boolean inVdb = false;
			for (Host host : parser.m_hosts) {
				String hostname = host.m_name;
				if (hostname.equals(m_hostname)) {
					if (host.m_port != m_port) {
						throw new RGMAPermanentException("VDB configuration file for expects this server to be on port " + host.m_port + " rather than "
								+ m_port + " as in the server configuration file. The VDB will not be started.");
					}
					inVdb = true;
					break;
				}
			}

			if (inVdb) {
				String vdbnameUpper = vdbname.toUpperCase();
				URL schema = null;
				List<URL> registries = new ArrayList<URL>();
				Set<String> hostnames = new HashSet<String>();

				for (Host host : parser.m_hosts) {
					String hostname = host.m_name;
					String prefix = "https://" + hostname + ":" + host.m_port + "/" + ServerConstants.WEB_APPLICATION_NAME + "/";
					if (host.m_isMasterSchema) {
						try {
							schema = new URL(prefix + ServerConstants.SCHEMA_SERVICE_NAME);
						} catch (MalformedURLException e) {
							throw new RGMAPermanentException("Bad URL generated", e);
						}
					}
					if (host.m_isRegistry) {
						try {
							registries.add(new URL(prefix + ServerConstants.REGISTRY_SERVICE_NAME));
						} catch (MalformedURLException e) {
							throw new RGMAPermanentException("Bad URL generated", e);
						}
					}
					hostnames.add(hostname.toUpperCase());
				}
				Service.setHostsForVDB(vdbnameUpper, hostnames);

				List<String> rules = new ArrayList<String>();
				for (Rule rule : parser.m_rules) {
					rules.add(rule.m_credentials + ":" + rule.m_action);
				}
				if (m_registryService != null) {
					m_registryService.createVDB(vdbname, registries);
				} else {
					m_registryCreateRequests.put(vdbnameUpper, new RegistryCreateRequest(vdbname, registries));
				}
				if (m_schemaService != null) {
					m_schemaService.createVDB(vdbname, schema, rules);
				} else {
					m_schemaCreateRequests.put(vdbnameUpper, new SchemaCreateRequest(vdbname, schema, rules));
				}
			} else {
				s_controlLogger.warn("VDB configuration file for " + vdbname + " does not include this host " + m_hostname);
				disableVDB(f);
			}
		} else { // VDB File removed
			disableVDB(f);
		}

	}

	private void disableVDB(File f) {
		String fname = f.getName();
		String vdbname = fname.substring(0, fname.indexOf('.'));
		String vdbnameUpper = vdbname.toUpperCase();
		if (m_registryService != null) {
			m_registryService.disableVDB(vdbname);
		} else {
			m_registryCreateRequests.remove(vdbnameUpper);
		}
		if (m_schemaService != null) {
			m_schemaService.disableVDB(vdbname);
		} else {
			m_schemaCreateRequests.remove(vdbnameUpper);
		}
	}

	private class RegistryCreateRequest {

		private List<URL> m_registries;

		private String m_vdbName;

		private RegistryCreateRequest(String vdbName, List<URL> registries) {
			m_vdbName = vdbName;
			m_registries = registries;
		}

		private void transmit() {
			try {
				m_registryService.createVDB(m_vdbName, m_registries);
			} catch (RGMAPermanentException e) {
				removeFileEntry(m_vdbName);
				s_controlLogger.error("ConfigurationException in VDBConfigurator processing: " + e);
			}
		}
	}

	private void removeFileEntry(String vdbName) {
		List<File> filesToRemove = new ArrayList<File>();
		for (File file : m_files.keySet()) {
			if (file.toString().equals(vdbName.toLowerCase() + ".xml")) {
				filesToRemove.add(file);
			}
		}
		for (File file : filesToRemove) {
			m_files.remove(file);
		}
	}

	private class SchemaCreateRequest {

		private String m_vdbName;
		private URL m_schema;
		private List<String> m_rules;

		private SchemaCreateRequest(String vdbName, URL schema, List<String> rules) {
			m_vdbName = vdbName;
			m_schema = schema;
			m_rules = rules;
		}

		private void transmit() {
			try {
				m_schemaService.createVDB(m_vdbName, m_schema, m_rules);
			} catch (RGMAPermanentException e) {
				removeFileEntry(m_vdbName);
				s_controlLogger.error("ConfigurationException in VDBConfigurator processing: " + e);
			}
		}

	}

}
