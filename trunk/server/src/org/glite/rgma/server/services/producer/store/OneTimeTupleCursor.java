package org.glite.rgma.server.services.producer.store;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.TupleSetWithLastTUID;

/**
 * Keeps track of tuples to be streamed for one-time queries (LATEST, HISTORY, STATIC).
 */
public class OneTimeTupleCursor implements TupleCursor {

	/** Database cursor ID. */
	private int m_cursorID;

	/** Reference to database containing tuples. */
	private TupleStoreDatabase m_databaseInstance;

	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger(TupleStoreConstants.TUPLE_STORE_LOGGER);

	private static final Logger s_securitylogger = Logger.getLogger("security");

	public OneTimeTupleCursor(SelectStatement query, TupleStoreDatabase databaseInstance) throws RGMAPermanentException, NumericException {
		m_databaseInstance = databaseInstance;
		m_cursorID = m_databaseInstance.openCursor(query);
		if (s_securitylogger.isInfoEnabled()) {
			s_securitylogger.info("OneTimeTupleCursor created for " + query);
		}
	}

	public void close() throws RGMAPermanentException {
		m_databaseInstance.closeCursor(m_cursorID);
	}

	public TupleSetWithLastTUID pop(int maxCount) throws RGMAPermanentException {
		TupleSetWithLastTUID rs = m_databaseInstance.fetch(m_cursorID, maxCount);
		if (LOG.isInfoEnabled()) {
			LOG.info("OneTimeTupleCursor popped " + rs.getTupleSet().size() + " tuples.");
		}
		return rs;
	}

}
