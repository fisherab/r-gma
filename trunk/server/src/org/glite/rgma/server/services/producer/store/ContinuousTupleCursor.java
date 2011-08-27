/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.sql.Constant;
import org.glite.rgma.server.services.sql.Expression;
import org.glite.rgma.server.services.sql.ExpressionOrConstant;
import org.glite.rgma.server.services.sql.OrderBy;
import org.glite.rgma.server.services.sql.SelectItem;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.TupleSetWithLastTUID;

/**
 * Keeps track of tuples to be streamed for a continuous query.
 */
public class ContinuousTupleCursor implements TupleCursor {
	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger(TupleStoreConstants.TUPLE_STORE_LOGGER);

	private static final Logger s_securitylogger = Logger.getLogger("security");

	/** ID of next tuple to send. */
	private int m_nextTupleID;

	/** Reference to database containing tuples. */
	private TupleStoreDatabase m_databaseInstance;

	/** SQL SELECT statement. */
	private SelectStatement m_query;

	private String m_vdbTableName;

	/** Expression to represent time being at least that from which tuples should be returned */
	private ExpressionOrConstant m_timeStampComparison;

	private static List<OrderBy> s_orderByTUID = new ArrayList<OrderBy>(0);

	static {
		s_orderByTUID.add(new OrderBy(ReservedColumns.RGMA_TUID_COLUMN_CONSTANT));
	}

	/**
	 * Creates a new ContinuousTupleCursor.
	 * 
	 * @param query
	 *            SQL SELECT statement (must be simple).
	 * @param startTimeMS
	 *            Start time in millis (see System#currentTimeMillis())
	 * @param databaseInstance
	 *            Reference to database containing tuples.
	 * @throws RGMAPermanentException
	 * @throws DatabaseException
	 */
	public ContinuousTupleCursor(SelectStatement query, long startTimeMS, TupleStoreDatabase databaseInstance, String vdbTableName, int lastTUID)
			throws RGMAPermanentException {
		m_databaseInstance = databaseInstance;
		m_query = new SelectStatement(query);

		/* Add RgmaTUID to query and ORDER BY RgmaTUID */
		m_query.getSelect().add(new SelectItem(ReservedColumns.RGMA_TUID_COLUMN_NAME));
		m_query.addOrderBy(s_orderByTUID);

		/* Find TUID to start from */
		String physicalTableName = query.getTables().get(0).getTableName();
		m_nextTupleID = m_databaseInstance.findFirstTupleID(physicalTableName, startTimeMS);
		boolean noTuples = m_nextTupleID == 0;
		if (lastTUID + 1 > m_nextTupleID) {
			m_nextTupleID = lastTUID + 1;
		}
		String startTimeStampString = new Timestamp(startTimeMS).toString();
		m_timeStampComparison = new Expression(">=", ReservedColumns.RGMA_TIMESTAMP_COLUMN_CONSTANT, new Constant(startTimeStampString, Constant.Type.STRING));

		m_vdbTableName = vdbTableName;
		if (s_securitylogger.isDebugEnabled()) {
			s_securitylogger.debug("ContinuousTupleCursor created for " + query);
		}
		if (LOG.isInfoEnabled()) {
			StringBuffer s = new StringBuffer("ContinuousTupleCursor " + hashCode() + " created for " + m_vdbTableName + ".");
			if (lastTUID != 0) {
				s.append(" Last TUID read previously by this consumer from this store was " + lastTUID + ".");
			}
			if (noTuples) {
				s.append(" No tuples in requested time frame in store.");
			}
			s.append(" Start from " + m_nextTupleID);
			LOG.info(s);
		}
	}

	/**
	 * @throws RGMAPermanentException
	 * @see TupleCursor#pop(int)
	 */
	public TupleSetWithLastTUID pop(int maxCount) throws RGMAPermanentException {
		SelectStatement select = new SelectStatement(m_query);
		/* Add to where clause to be greater than tupleID and greater than or equal to the timestamp */
		ExpressionOrConstant tupleIDPredicate = new Expression("AND", new Expression(">=", ReservedColumns.RGMA_TUID_COLUMN_CONSTANT, new Constant(
				m_nextTupleID + "", Constant.Type.NUMBER)), m_timeStampComparison);

		if (select.getWhere() != null) {
			select.addWhere(new Expression("AND", select.getWhere(), tupleIDPredicate));
		} else {
			select.addWhere(tupleIDPredicate);
		}

		TupleSetWithLastTUID poppedTuples = m_databaseInstance.getContinuous(select, maxCount);

		/* get ID of last tuple read. It may be zero if ResultSet is empty */
		int n = poppedTuples.getLastTUID();
		TupleSet ts = poppedTuples.getTupleSet();
		if (n != 0) {
			m_nextTupleID = n + 1;
		}
		if (LOG.isInfoEnabled()) {
			if (ts.size() > 0) {
				LOG.debug("ContinuousTupleCursor " + hashCode() + " popped " + ts.size() + " tuples. Next TUID to seek is " + m_nextTupleID);
			}
		}
		return poppedTuples;
	}

	/**
	 * close
	 */
	public void close() {
	/*
	 * This function exists because of the interface. However it has nothing to do as the constructor created nothing
	 * that needs cleaning up and each pop results in a call to get the next set of tuples from the history tuple store
	 * knowing the next tuple id to start from.
	 */
	}
}
