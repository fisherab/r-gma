/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.streaming;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import javax.net.ssl.SSLContext;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.producer.RunningQuery;
import org.glite.rgma.server.services.producer.store.TupleStore;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.TupleSetEnvelope;

/**
 * Provides data to be sent on an outgoing streaming connection. A StreamingSource instance is created for each new
 * connection opened by the streaming sender to a streaming receiver. The streaming sender takes bytes provided by the
 * StreamingSource and writes them to the socket.
 * <p>
 * Synchronization is necessary on those methods which affect the internal list of queries since these may be called by
 * different threads.
 */

/* TODO is there too much synchronization as described above ? */

public class StreamingSource extends StreamingSSLEngine {

	private static final int PROTNUM = 2;

	/** If <code>true</code>, this streaming source will accept new queries. */
	private boolean m_active;

	/** ByteBuffer containing the header bytes for the streaming protocol. */
	private ByteBuffer m_header;

	/** If <code>true</code>, header bytes have been sent. */
	private boolean m_headerWritten;

	/** LinkedList of queries whose results are to be sent using this StreamingSource. */
	private final Queue<RunningQuery> m_queries;

	private final StreamingProperties m_streamingProps;

	private int m_optimalPackeSize;

	private long m_dropTime;

	private long m_periodToKeepRedundantSourceMillis;

	/* This read buffer is shared because it is never actually used */
	private static ByteBuffer s_readBuffer;

	/**
	 * Constructor.
	 * 
	 * @param streamingProps
	 *            Streaming endpoint and protocol version.
	 * @param bufferSize
	 *            Size of the write buffer in bytes.
	 * @param spservice
	 * @param ppservice
	 * @param odpservice
	 * @param sslContext
	 * @param allocateDirect
	 * @param currentResultSetRetry
	 */
	public StreamingSource(StreamingProperties streamingProps, int optimalPacketSize, SSLContext sslContext, boolean allocateDirect,
			long periodToKeepRedundantSourceMillis) throws RGMAPermanentException {
		LOG = Logger.getLogger(StreamingConstants.STREAMING_SENDER_LOGGER);
		m_sslEngine = sslContext.createSSLEngine(streamingProps.getStreamingHost(), streamingProps.getStreamingPort());
		m_handshakeStatus = m_sslEngine.getHandshakeStatus();
		m_sslEngine.setUseClientMode(true);

		int netBS = m_sslEngine.getSession().getPacketBufferSize();
		if (allocateDirect) {
			m_encryptedWriteBuffer = ByteBuffer.allocateDirect(netBS);
		} else {
			m_encryptedWriteBuffer = ByteBuffer.allocate(netBS);
		}
		m_encryptedReadBuffer = ByteBuffer.allocate(netBS);

		/* This read buffer must be of the approved size though it is never used */
		int appBS = m_sslEngine.getSession().getApplicationBufferSize();
		if (s_readBuffer == null) {
			s_readBuffer = ByteBuffer.allocate(appBS);
		} else if (appBS > s_readBuffer.capacity()) {
			s_readBuffer = ByteBuffer.allocate(appBS);
		}
		m_readBuffer = s_readBuffer;

		m_optimalPackeSize = optimalPacketSize;
		m_streamingProps = streamingProps;
		m_periodToKeepRedundantSourceMillis = periodToKeepRedundantSourceMillis;
		m_queries = new LinkedList<RunningQuery>();
		m_headerWritten = false;
		m_header = new TupleEncoder(PROTNUM).getHeader();
		m_bytesSinceHandshake = 0;
		m_active = true;
	}

	/**
	 * Add a query to the streaming source.
	 * 
	 * @param query
	 *            RunningQuery to add.
	 */
	public synchronized boolean addQuery(RunningQuery query) {
		if (m_active) {
			m_queries.add(query);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Added RunningQuery:" + query + " to StreamingSource with status " + m_engineOpStatus + " " + m_handshakeStatus);
			}
			return true;
		} else {
			return false;
		}
	}

	public StreamingProperties getStreamingProperties() {
		return m_streamingProps;
	}

	public synchronized boolean isTupleStoreUsed(TupleStore store) {
		for (RunningQuery q : m_queries) {
			if (q.getTupleStore() == store) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Pop bytes to be sent on the streaming connection.
	 * 
	 * @return buffer Buffer containing bytes to be sent on the streaming connection, positioned to be written from the
	 *         current position, or <code>null</code> if this streaming source has completed sending all of its queries
	 *         and is now closed.
	 * @throws RGMAPermanentException
	 */
	public synchronized ByteBuffer popBytes() throws RGMAPermanentException {
		RunningQuery query = null;
		if (!m_active && m_sslEngine.isOutboundDone() && m_encryptedWriteBuffer.position() == 0) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(this + " is closing down");
			}
			return null;
		}
		boolean active = m_queries.size() > 0 || m_writeBuffers.size() > 0;
		if (active) {
			m_dropTime = 0;
		} else {
			if (m_dropTime == 0) {
				m_dropTime = System.currentTimeMillis() + m_periodToKeepRedundantSourceMillis;
				if (LOG.isDebugEnabled()) {
					LOG.debug(this + " is not currently active - wait for " + m_periodToKeepRedundantSourceMillis + " ms before closing");
				}
			} else if (System.currentTimeMillis() > m_dropTime) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Sending closeOutBound for " + this);
				}
				m_active = false;
				m_sslEngine.closeOutbound();
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug(this + " still not active - wait for " + (m_dropTime - System.currentTimeMillis()) + " ms before closing");
				}
			}
		}
		if (!m_headerWritten) {
			m_writeBuffers.add(m_header);
			m_headerWritten = true;
		}
		boolean dataFound = false;
		RunningQuery sentinel = null; /* Will be used to detect once round the queue */
		int bytesToWrite = 0;
		for (ByteBuffer b : m_writeBuffers) {
			bytesToWrite += b.position();
		}
		while (bytesToWrite < m_optimalPackeSize) {
			query = m_queries.poll();
			if (query == null) {
				break;
			}
			boolean atSentinel = false;
			if (sentinel == null) {
				sentinel = query;
				atSentinel = true;
			} else if (sentinel == query) {
				if (!dataFound) {
					m_queries.add(query);
					break;
				}
				dataFound = false;
				atSentinel = true;
			}
			if (query.isActive()) {
				try {
					TupleSetEnvelope results = query.pop();
					TupleSet ts = results.getTupleSet();
					boolean eof = ts.isEndOfResults();
					if (ts.size() != 0 || eof) {
						if (LOG.isDebugEnabled()) {
							LOG.debug(ts.size() + " tuples for " + query + (eof ? " *EOF*" : ""));
						}
						ByteBuffer currentResultSet = new TupleEncoder(PROTNUM).encode(results);
						bytesToWrite += currentResultSet.position();
						m_writeBuffers.add(currentResultSet);
						if (ts.size() != 0) {
							dataFound = true;
						}
						if (eof) {
							query = null;
						}
					}
				} catch (RGMAPermanentException e) {
					LOG.warn("Failed to pop tuples from store for " + query + ". " + e.getMessage() + " - results will be discarded");
					/* stop this happening again */
					query.abort();
					query = null;
				}
				if (query != null) {
					m_queries.add(query);
				} else if (atSentinel) {
					/* Need a new sentinel */
					sentinel = null;
				}
			} else if (atSentinel) {
				/* Need a new sentinel */
				sentinel = null;
			}
		}
		return wrap();
	}

	/**
	 * Prepare the StreamingSource to send bytes on a newly created connection. This method is called if the streaming
	 * sender is forced to recreate the socket connection. The streaming source must discard any partially complete
	 * result sets it has sent and re-send the complete result set. It must also re-send the header bytes. However we
	 * must not keep retrying the same resultSet - so watch the number of retrys
	 * 
	 * @throws RGMAPermanentException
	 */
	public synchronized void reset() throws RGMAPermanentException {
		m_writeBuffers.clear();
		m_encryptedWriteBuffer.clear();
		m_readBuffer.clear();
		m_encryptedReadBuffer.clear();
		m_headerWritten = false;
		m_header = new TupleEncoder(PROTNUM).getHeader();
	}

	@Override
	public synchronized String toString() {
		return "Source for " + m_streamingProps.toString() + " has " + m_queries.size() + " queries and " + m_writeBuffers.size() + " write buffers bytes open";
	}

	@Override
	public void pushBytes() throws RGMAPermanentException {
		unwrap();
	}
}
