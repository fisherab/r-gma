/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer;

import org.glite.rgma.server.services.producer.store.TupleCursor;
import org.glite.rgma.server.services.producer.store.TupleStore;
import org.glite.rgma.server.services.streaming.StreamingSender;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.TupleSetEnvelope;
import org.glite.rgma.server.system.TupleSetWithLastTUID;

/**
 * Represents a Producer's connection to a consumer running a query on it.
 */
public class RunningQuery {

	private final StreamingProperties m_streamingProps;

	/** Tuple cursor for accessing the results of the query */
	private final TupleCursor m_cursor;

	/** Number of tuples to aim for in each result set. */
	private final int m_chunkSize;

	/** If <code>true</code>, query is active. */
	private boolean m_active;

	private ResourceEndpoint m_producer;

	private ResourceEndpoint m_consumer;

	private String m_query;

	private boolean m_iscontinuous;

	private TupleStore m_tupleStore;

	public ResourceEndpoint getProducer() {
		return m_producer;
	}

	public String toString() {
		return "RQ <" + m_query + "> " + m_producer + "->" + m_consumer;
	}

	private boolean m_closingDown;

	private boolean m_moreDataToMove = true;

	private StreamingSender m_streamingSender;

	private String m_firstVdbTableName;

	public RunningQuery(StreamingProperties streamingProps, TupleCursor cursor, TupleStore tuplestore, int chunkSize, ResourceEndpoint producer,
			ResourceEndpoint consumer, String query, boolean iscontinuous, StreamingSender sender, String firstVdbTableName) {
		m_streamingProps = streamingProps;
		m_cursor = cursor;
		m_chunkSize = chunkSize;
		m_producer = producer;
		m_consumer = consumer;
		m_query = query;
		m_iscontinuous = iscontinuous;
		m_tupleStore = tuplestore;
		m_active = true;
		m_streamingSender = sender;
		m_firstVdbTableName = firstVdbTableName;
	}

	public StreamingProperties getStreamingProperties() {
		return m_streamingProps;
	}

	/**
	 * Returns consumer query
	 * 
	 * @return
	 */
	public String getConsumerQuery() {
		return m_query;
	}

	public TupleStore getTupleStore() {
		return m_tupleStore;
	}

	public synchronized void abort() {
		m_active = false;
	}

	public synchronized boolean isActive() {
		return m_active;
	}

	/** Get a chunk of results for the query */
	public synchronized TupleSetEnvelope pop() throws RGMAPermanentException {
		TupleSet ts;
		if (m_active) {
			TupleSetWithLastTUID rs = m_cursor.pop(m_chunkSize);
			ts = rs.getTupleSet();
			if (ts.isEndOfResults()) {
				m_cursor.close();
				m_active = false;
			}
			if (m_iscontinuous) {
				int n = rs.getLastTUID();
				if (n != 0) {
					m_tupleStore.setConsumerTUID(m_firstVdbTableName, m_consumer, n);
				} else if (m_closingDown) {
					m_moreDataToMove = false;
					ts.setEndOfResults(true);
				}
			}
		} else {
			if (m_cursor != null) {
				m_cursor.close();
			}
			ts = new TupleSet();
			ts.setEndOfResults(true);

		}
		return new TupleSetEnvelope(ts, m_producer, m_consumer, m_query);
	}

	/**
	 * This returns true while there is more data to extract from the continuous cursor and pass on to the
	 * StreamingSender. At its first call it will return false, but will set the m_closingDown flag. Subsequently when
	 * pop() is called by the StreamingSender it will set the m_moreDataToMove to false as soon as it gets no data from
	 * the cursor. In order to provoke a pop dataAddedToTupleStore is called.
	 */
	public synchronized boolean moreDateToMove() {
		m_closingDown = true;
		m_streamingSender.dataAddedToTupleStore(m_tupleStore);
		return m_moreDataToMove;
	}
}
