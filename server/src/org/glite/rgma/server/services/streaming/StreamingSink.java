/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.streaming;

import java.nio.ByteBuffer;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.consumer.RunningReply;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSetEnvelope;

/**
 * Handles data from an incoming streaming connection.
 * 
 * The streaming receiver creates an instance of StreamingSink for each new connection it accepts.
 * All bytes read from the associated socket are passed to the StreamingSink for handling.
 * <p>
 * The first task of the StreamingSink is to identify the streaming protocol being used. This is
 * done by reading the first four bytes of the stream (the 'header') and converting them into an
 * integer. If this integer is positive, it is interpreted as the resource ID of a consumer for
 * which all subsequent results are intended. This behaviour is required to maintain compatibility
 * with the old 'classic' R-GMA streaming protocol.
 * <p>
 * If the header integer is negative, it is treated as a streaming protocol version number and an
 * appropriate TupleDecoder is created to decode subsequent bytes into result sets. For each
 * complete result set, the list of RunningReply objects is traversed to find the one which matches
 * the source and target endpoints and the SQL query from the result set.
 */
public class StreamingSink extends StreamingSSLEngine {

	/** Object for turning the byte stream into result sets */
	private TupleDecoder m_decoder;

	/** Buffer to store leading integer from streaming message */
	private final ByteBuffer m_header;

	/** List of running replies to receive results */
	private final List<RunningReply> m_replies;

	/**
	 * Constructor.
	 * 
	 * @param protocolFactory
	 *            Factory for retrieving streaming protocol implementations.
	 * @param replies
	 *            RunningReply objects to receive results. Access to this list must be synchronized
	 *            on the list object since although the streaming receiver operates in a single
	 *            thread, Consumers may call <code>addReply</code> from other threads.
	 * @param context
	 */
	public StreamingSink(List<RunningReply> replies, SSLContext sslContext, boolean allocateDirect)
			throws RGMAPermanentException {
		LOG = Logger.getLogger(StreamingConstants.STREAMING_RECEIVER_LOGGER);
		m_sslEngine = sslContext.createSSLEngine();
		m_handshakeStatus = m_sslEngine.getHandshakeStatus();
		m_sslEngine.setUseClientMode(false);
		m_sslEngine.setNeedClientAuth(true);

		int netBS = m_sslEngine.getSession().getPacketBufferSize();
		if (allocateDirect) {
		m_encryptedWriteBuffer = ByteBuffer.allocateDirect(netBS);
		} else {
			m_encryptedWriteBuffer = ByteBuffer.allocate(netBS);
		}
		m_encryptedReadBuffer = ByteBuffer.allocate(netBS);
		
		int appBS = m_sslEngine.getSession().getApplicationBufferSize();
		m_readBuffer = ByteBuffer.allocate(appBS);

		m_replies = replies;
		m_header = ByteBuffer.allocate(4);
		m_decoder = null;
	}

	/**
	 * Process streaming data.
	 * 
	 * @param bytes
	 *            Streaming data buffer.
	 */
	public void pushBytes() throws RGMAPermanentException {
		unwrap();
		m_readBuffer.flip();
		if (m_decoder == null) {
			// We are reading the header
			readHeader(m_readBuffer);
		}
		if (m_decoder != null) {
			// We are reading results
			readResults(m_readBuffer);
		}
		m_readBuffer.compact();
	}

	@Override
	public String toString() {
		return "StreamingSink " + hashCode();
	}

	/**
	 * Attempt to read the four byte header from the streaming message.
	 * 
	 * @param bytes
	 *            Data from the streaming connection.
	 */
	private void readHeader(ByteBuffer bytes) throws RGMAPermanentException {
		if (bytes.remaining() >= m_header.remaining()) {
			byte[] data = new byte[m_header.remaining()];
			bytes.get(data);
			m_header.put(data);
		} else {
			m_header.put(bytes);
		}

		if (m_header.remaining() == 0) {
			m_header.flip();
			int headerInt = m_header.getInt();
			m_decoder = new TupleDecoder(headerInt);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Created decoder of type " + headerInt);
			}
		}
	}

	/**
	 * Attempt to read result sets from the streaming message.
	 * 
	 * @param bytes
	 *            Data from the streaming connection.
	 */
	private void readResults(ByteBuffer bytes) throws RGMAPermanentException {
		m_decoder.pushBytes(bytes);
		List<TupleSetEnvelope> results = m_decoder.popResults();
		LOG.debug("Result set list from decoder contains " + results.size() + " result sets");
		for (TupleSetEnvelope rs : results) {
			ResourceEndpoint source = rs.getSource();
			ResourceEndpoint target = rs.getTarget();
			String query = rs.getQuery();

			synchronized (m_replies) {
				boolean foundReply = false;
				
				for (RunningReply reply : m_replies) {
					if (reply.matches(source, target, query)) {
						reply.push(rs.getTupleSet());
						foundReply = true;
						if (LOG.isDebugEnabled()) {
							LOG.debug("Pushed " + reply);
						}
						break;
					}
				}
				if (!foundReply) {
					/*
					 * Can't find a reply for this result set. Not a very serious error as producer
					 * may not be aware the consumer has died or been aborted. It will find out soon
					 * enough. Just log it.
					 */
					LOG.debug("No matching reply found for streamed results " + source + "->" + target + " for " + query);
				}
			}
		}
	}

	public ByteBuffer popBytes() throws RGMAPermanentException {
		return wrap();
	}
}
