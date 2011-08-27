/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * Consumer's streaming server connection details.
 */
public class StreamingProperties {
	/** Number of tuples to send in each streaming chunk. */
	private final int m_chunkSize;

	/** Port number of streaming server. */
	private final int m_streamingPort;

	/** Version number of the streaming protocol to use. */
	private final int m_streamingProtocol;

	/** Host of streaming server. */
	private final String m_streamingHost;

	/**
	 * @param streamingHost
	 * @param streamingPort
	 * @param chunkSize
	 * @param streamingProtocol
	 */
	public StreamingProperties(String streamingHost, int streamingPort, int chunkSize, int streamingProtocol) {
		m_streamingHost = streamingHost;
		m_streamingPort = streamingPort;
		m_chunkSize = chunkSize;
		m_streamingProtocol = streamingProtocol;
	}

	public int getChunkSize() {
		return m_chunkSize;
	}

	public int getStreamingPort() {
		return m_streamingPort;
	}

	public int getStreamingProtocol() {
		return m_streamingProtocol;
	}

	public String getStreamingHost() {
		return m_streamingHost;
	}

	@Override
	public String toString() {
		return m_streamingHost + ":" + m_streamingPort + "(" + m_streamingProtocol + ")";
	}
}
