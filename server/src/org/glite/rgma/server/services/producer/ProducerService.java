package org.glite.rgma.server.services.producer;

import java.util.List;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.Service;
import org.glite.rgma.server.services.producer.store.TupleStoreDetails;
import org.glite.rgma.server.services.producer.store.TupleStoreManager;
import org.glite.rgma.server.services.resource.ResourceManagementService;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserSystemContext;
import org.glite.rgma.server.system.Storage.StorageType;

public abstract class ProducerService extends ResourceManagementService {

	private static TupleStoreManager s_DBTupleStoreManager;

	private static final Logger LOG = Logger.getLogger("name");

	protected ProducerService(String name) throws RGMAPermanentException {
		super(name);
		s_DBTupleStoreManager = TupleStoreManager.getInstance(StorageType.DB);
	}

	/**
	 * Request a producer to execute a query & start streaming Tuples to consumer
	 */
	public TupleSet start(int resourceId, String select, QueryProperties queryProps, TimeInterval timeout, ResourceEndpoint consumer,
			StreamingProperties streamingProps, UserSystemContext context) throws UnknownResourceException, RGMAPermanentException, RGMAPermanentException,
			NumericException {
		TupleSet rs = null;
		try {
			ProducerResource pResource = (ProducerResource) getResource(resourceId);
			checkContactable(pResource, Api.SYSTEM_API);
			rs = pResource.start(select, timeout, queryProps, consumer, streamingProps, context);
		} catch (UnknownResourceException e) {
			m_logger.info("Start of producer resource: " + resourceId + " failed. " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.info("Start of producer resource: " + resourceId + " failed. " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Producer " + resourceId + " started " + queryProps + " query for consumer " + consumer);
		}
		rs.setEndOfResults(true);
		return rs;
	}

	/**
	 * Request the producer to stop streaming tuples to the specified consumer & abort any running query
	 * 
	 * @param resourceId
	 * @param consumer
	 * @throws UnknownResourceException
	 * @throws RGMAPermanentException
	 */
	public void abort(int resourceId, ResourceEndpoint consumer) throws UnknownResourceException, RGMAPermanentException {
		try {
			ProducerResource pResource = (ProducerResource) getResource(resourceId);
			checkContactable(pResource, Api.SYSTEM_API);
			pResource.abortQuery(consumer);
		} catch (UnknownResourceException e) {
			m_logger.info("Abort of producer resource: " + resourceId + " failed " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Abort of producer resource: " + resourceId + " failed " + e.getFlattenedMessage());
			throw e;
		}
		if (m_logger.isInfoEnabled()) {
			m_logger.info("Producer resource: " + resourceId + " aborted query from " + consumer);
		}
	}

	/**
	 * Permanently deletes a permanent tuple store. The tuple store is only deleted if it is currently not being used by
	 * another producer.
	 */
	public static void dropTupleStore(UserContext context, String logicalName) throws RGMAPermanentException {
		try {
			Service.checkUserInputValid(logicalName);
			s_DBTupleStoreManager.dropTupleStore(context.getDN(), logicalName);
		} catch (RGMAPermanentException e) {
			LOG.info("Failed to Drop Tuple Store: for logical name: " + logicalName + " - " + e.getMessage());
			throw e;
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Dropped Tuple Store for logical name : " + logicalName);
		}
	}

	public int getHistoryRetentionPeriod(int resourceId, String tableName) throws RGMAPermanentException, UnknownResourceException, RGMAPermanentException {
		try {
			ProducerResource producer = (ProducerResource) getResource(resourceId);
			checkContactable(producer, Api.USER_API);
			int hrpsecs = producer.getHistoryRetentionPeriod(tableName);
			if (m_logger.isInfoEnabled()) {
				m_logger.info("Got HRP of " + hrpsecs + " for " + producer + " " + tableName);
			}
			return hrpsecs;
		} catch (UnknownResourceException e) {
			m_logger.info("Failed to get History Retention Period from primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		} catch (RGMAPermanentException e) {
			m_logger.error("Failed to get History Retention Period from primary resource: " + resourceId + " - " + e.getMessage());
			throw e;
		}
	}

	/**
	 * Returns a list of all the permanent tuple stores in this service and their physical location.
	 * 
	 * @param userDN
	 *            The id of the resource publishing to the specified table
	 * @param tableName
	 *            Table to retrieve its LRP.
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 * @throws UnknownResourceException
	 */
	public static TupleSet listTupleStores(UserContext context) throws RGMAPermanentException, RGMAPermanentException {
		try {
			List<TupleStoreDetails> stores = s_DBTupleStoreManager.listTupleStores(context.getDN());
			TupleSet rs = new TupleSet();
			for (TupleStoreDetails t : stores) {
				String ln = t.getLogicalName();
				String h = t.supportsHistory() ? "true" : "false";
				String l = t.supportsLatest() ? "true" : "false";
				rs.addRow(new String[] { ln, h, l });
			}
			if (LOG.isInfoEnabled()) {
				LOG.info("listTupleStores returns " + rs.size() + " tuple stores");
			}
			rs.setEndOfResults(true);
			return rs;
		} catch (RGMAPermanentException e) {
			LOG.error("Failed to list permanent Tuple Stores " + e.getFlattenedMessage());
			throw e;
		}
	}
}
