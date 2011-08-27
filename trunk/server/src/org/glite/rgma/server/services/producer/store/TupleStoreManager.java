/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteProducer;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.producer.primary.PrimaryProducerService;
import org.glite.rgma.server.services.producer.secondary.SecondaryProducerService;
import org.glite.rgma.server.services.resource.Resource;
import org.glite.rgma.server.services.streaming.StreamingSender;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.Storage.StorageType;

/**
 * Manages the creation/opening and dropping of tuple stores.
 */
public class TupleStoreManager {
	/**
	 * TimerTask which cleans up old tuples. It also checks for tuplestores owned by non-existent resources and closes
	 * them.
	 */
	private class TupleCleanupTask extends TimerTask {
		public void run() {
			Collection<TupleStore> tss;
			synchronized (m_tupleStores) {
				tss = new ArrayList<TupleStore>(m_tupleStores);
			}
			for (TupleStore ts : tss) {
				try {
					ts.cleanUpTables();
					if (LOG.isDebugEnabled()) {
						LOG.debug("CleanedUp tuple store " + ts);
					}
					ResourceEndpoint re = ts.getDetails().getEndpoint();
					try {
						RemoteProducer.ping(re.getURL(), re.getResourceID());
					} catch (UnknownResourceException e) {
						LOG.warn("TupleStore was supporting " + re + " which no longer exists - so closing it");
						closeTupleStore(ts);
					} catch (RemoteException e) {
						LOG.error(e);
					} catch (RGMATemporaryException e) {
						LOG.error(e);
					}
				} catch (RGMAPermanentException e) {
					LOG.error(e);
				}
			}
		}
	}

	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger(TupleStoreConstants.TUPLE_STORE_LOGGER);
	private static final String s_ppPath = "/" + ServerConstants.WEB_APPLICATION_NAME + "/" + ServerConstants.PRIMARY_PRODUCER_SERVICE_NAME;

	private static final String s_spPath = "/" + ServerConstants.WEB_APPLICATION_NAME + "/" + ServerConstants.SECONDARY_PRODUCER_SERVICE_NAME;

	/** Map of permanent open tuple stores with a key of the form <DN>.<logicalName> */
	private Map<String, TupleStore> m_permanentTupleStores;

	/** List of all open tuple stores */
	private List<TupleStore> m_tupleStores;

	/** A static reference to an DB instance of this class. */
	private static TupleStoreManager s_DBservice;

	/** A static reference to a MEM instance of this class. */
	private static TupleStoreManager s_MEMservice;

	private static Object s_instanceLock = new Object();

	/** Implementation of DatabaseInstance for database tuple store. */
	private static final String DATABASE_MANAGER_DB_IMPL = "org.glite.rgma.server.services.producer.store.MySQLTupleStoreDatabase";

	/** Implementation of DatabaseInstance for database tuple store. */
	private static final String DATABASE_MANAGER_MEM_IMPL = "org.glite.rgma.server.services.producer.store.HSQLDBTupleStoreDatabase";

	/** Thread used to asynchronously complete the tuple cleanup process */
	private Timer m_tupleCleanupThread;

	private TupleStoreDatabase m_databaseInstance;

	private StorageType m_type;

	/** When this is exceeded an error is thrown */
	private long m_maxHistoryTuples;

	private StreamingSender m_streamingSender;

	private static PrimaryProducerService s_primaryProducerService;

	private static SecondaryProducerService s_secondaryProducerService;

	public static void dropInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_MEMservice != null) {
				s_MEMservice.shutdown();
				s_MEMservice = null;
			}
			if (s_DBservice != null) {
				s_DBservice.shutdown();
				s_DBservice = null;
			}
		}
	}

	/**
	 * Gets the singleton instance of TupleStoreManager class
	 * 
	 * @return
	 * @throws ConfigurationException
	 * @throws TupleStoreException
	 */
	public static TupleStoreManager getInstance(StorageType storageType) throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (storageType.equals(StorageType.DB)) {
				if (s_DBservice == null) {
					s_DBservice = new TupleStoreManager(storageType);
				}
				return s_DBservice;
			} else {
				if (s_MEMservice == null) {
					s_MEMservice = new TupleStoreManager(storageType);
				}
				return s_MEMservice;
			}
		}
	}

	public static void setPrimaryProducerService(PrimaryProducerService s) {
		s_primaryProducerService = s;
	}

	public static void setSecondaryProducerService(SecondaryProducerService s) {
		s_secondaryProducerService = s;
	}

	/**
	 * Creates a new TupleStoreManager. Instantiates a new permanent tuple store list.
	 * 
	 * @throws ConfigurationException
	 * @throws TupleStoreException
	 *             If the permanent tuple store list could not be instantiated.
	 */
	private TupleStoreManager(StorageType storageType) throws RGMAPermanentException {
		try {
			m_type = storageType;
			int intervalMs;
			ServerConfig config = ServerConfig.getInstance();
			if (storageType.equals(StorageType.DB)) {
				m_databaseInstance = (TupleStoreDatabase) Class.forName(DATABASE_MANAGER_DB_IMPL).newInstance();
				intervalMs = config.getInt(ServerConstants.TUPLESTOREMANAGER_DB_CLEANUP_INTERVAL_SECS) * 1000;
				m_maxHistoryTuples = config.getLong(ServerConstants.TUPLESTOREMANAGER_DB_MAX_HISTORY_TUPLES);
			} else if (storageType.equals(StorageType.MEM)) {
				m_databaseInstance = (TupleStoreDatabase) Class.forName(DATABASE_MANAGER_MEM_IMPL).newInstance();
				intervalMs = config.getInt(ServerConstants.TUPLESTOREMANAGER_MEM_CLEANUP_INTERVAL_SECS) * 1000;
				m_maxHistoryTuples = config.getLong(ServerConstants.TUPLESTOREMANAGER_MEM_MAX_HISTORY_TUPLES);
			} else {
				throw new RGMAPermanentException("Invalid StorageType");
			}
			m_permanentTupleStores = new HashMap<String, TupleStore>();
			m_tupleStores = new ArrayList<TupleStore>();
			m_tupleCleanupThread = new Timer(true);
			m_tupleCleanupThread.schedule(new TupleCleanupTask(), intervalMs, intervalMs);
			m_streamingSender = StreamingSender.getInstance();
		} catch (IllegalAccessException e) {
			throw new RGMAPermanentException(e);
		} catch (InstantiationException e) {
			throw new RGMAPermanentException(e);
		} catch (ClassNotFoundException e) {
			throw new RGMAPermanentException(e);
		}
	}

	/**
	 * Closes a tuple store. If this is a temporary tuple store, any resources used to store tuples are deleted/freed.
	 * 
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 */
	public void closeTupleStore(TupleStore store) throws RGMAPermanentException {
		TupleStoreDetails details = store.getDetails();
		String logicalName = details.getLogicalName();
		boolean present;
		synchronized (m_tupleStores) {
			if (logicalName.length() > 0) {
				String key = details.getOwnerDN() + '.' + logicalName;
				synchronized (m_permanentTupleStores) {
					m_permanentTupleStores.remove(key);
				}
			}
			present = m_tupleStores.remove(store);
		}
		if (present) {
			store.close();
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Tuple store " + details + " closed.");
		}
	}

	/**
	 * Creates a new tuple store using the specified storage, capable of answering the specified history and/or latest
	 * queries. The latter will be dropped on a call to TupleStore#close().
	 */
	public TupleStore createTupleStore(String DN, String logicalName, boolean isLatest, ResourceEndpoint endpoint) throws RGMAPermanentException,
			RGMAPermanentException {
		String key = DN + '.' + logicalName;
		synchronized (m_tupleStores) {
			synchronized (m_permanentTupleStores) {
				if (logicalName.length() > 0) {
					if (m_type != StorageType.DB) {
						throw new RGMAPermanentException("A logical name can only be used with DB storage");
					}
					if (m_permanentTupleStores.containsKey(key)) {
						ResourceEndpoint re = m_permanentTupleStores.get(key).getDetails().getEndpoint();
						String servicePath = re.getURL().getPath();
						int resourceId = re.getResourceID();
						Resource r = null;
						if (servicePath.equals(s_ppPath)) {
							try {
								r = s_primaryProducerService.getResource(resourceId);
							} catch (UnknownResourceException e) {}
						} else if (servicePath.equals(s_spPath)) {
							try {
								r = s_secondaryProducerService.getResource(resourceId);
							} catch (UnknownResourceException e) {}
						} else {
							throw new RGMAPermanentException("TupleStores may only be used by primary and secondary producers");
						}
						if (r != null && !r.isClosed() && !r.isDestroyed()) {
							throw new RGMAPermanentException("You are already using this tuple store identified by DN and logical name by " + servicePath + " "
									+ resourceId);
						}
					}
				}
				TupleStoreDetails details = new TupleStoreDetails(m_type, logicalName, DN, isLatest, endpoint);
				TupleStore store = new TupleStore(m_databaseInstance, details, m_maxHistoryTuples, m_streamingSender);
				if (logicalName.length() > 0) {
					m_permanentTupleStores.put(key, store);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Added " + logicalName + " to set of open permanent tuple stores. There are now: " + m_permanentTupleStores.size());
					}
				}
				m_tupleStores.add(store);
				return store;
			}
		}
	}

	/**
	 * Drops the tuple store with the given name if it exists. Only permanent (database) tuple stores can be dropped. If
	 * the tuple store does not exist, no exception is thrown.
	 * 
	 * @param userDN
	 *            Distinguished name of user making call.
	 * @param logicalName
	 *            Name of store to drop.
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 *             If the user is not authorized to drop the given tuple store.
	 * @throws TupleStoreException
	 *             If the tuple store can't be dropped (e.g. the database can't be contacted).
	 */
	public void dropTupleStore(String DN, String logicalName) throws RGMAPermanentException, RGMAPermanentException {
		String key = DN + '.' + logicalName;
		synchronized (m_permanentTupleStores) {
			if (m_permanentTupleStores.containsKey(key)) {
				throw new RGMAPermanentException("Tuple store is in use.");
			}
			m_databaseInstance.dropTupleStore(DN, logicalName);
		}
	}

	/**
	 * Lists the permanent tuple stores available to this manager. This always references DB not MEM.
	 * 
	 * @param userDN
	 *            Distinguished name of tuple store owner
	 * @return A list of tuple store details.
	 * @throws RGMAPermanentException
	 */
	public List<TupleStoreDetails> listTupleStores(String userDN) throws RGMAPermanentException, RGMAPermanentException {
		List<TupleStoreDetails> tsds = m_databaseInstance.listTupleStores(userDN);
		for (TupleStoreDetails tsd : tsds) {
			String key = tsd.getOwnerDN() + '.' + tsd.getLogicalName();
			TupleStore store = null;
			synchronized (m_permanentTupleStores) {
				store = m_permanentTupleStores.get(key);
			}
			if (store != null) {
				tsd.setEndpoint(store.getDetails().getEndpoint());
			}
		}
		return tsds;
	}

	private void shutdown() throws RGMAPermanentException {
		m_tupleCleanupThread.cancel();
		m_databaseInstance.shutdown();
	}
}
