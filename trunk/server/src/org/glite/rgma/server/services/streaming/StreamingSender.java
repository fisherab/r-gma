/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.streaming;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.producer.RunningQuery;
import org.glite.rgma.server.services.producer.store.TupleStore;
import org.glite.rgma.server.servlets.RGMAContextWrapper;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.StreamingProperties;

/**
 * Sends data on outgoing streaming connections on behalf of Primary and Secondary Producer resources.
 */
public final class StreamingSender extends StreamingEndpoint {

	private class Status {

		public Set<StreamingSource> m_registeredKeySources;

		public List<StreamingSource> m_sources;

		public Set<StreamingSource> m_sourcesToConnect;

		public String m_status;

		public long m_timeSinceLastCleanupMillis;

		public Set<StreamingSource> m_unregisteredKeySources;

		private Status() {
			m_registeredKeySources = m_sourcesToConnect = m_unregisteredKeySources = new HashSet<StreamingSource>();
			m_sources = new ArrayList<StreamingSource>();
			m_timeSinceLastCleanupMillis = System.currentTimeMillis();
		}
	}

	protected static final Logger s_securityLogger = Logger.getLogger(ServerConstants.SECURITY_LOGGER);

	private static final Object s_instanceLock = new Object();

	/** Singleton instance */
	private static StreamingSender s_sender;

	public static void dropInstance() {
		synchronized (s_instanceLock) {
			if (s_sender != null) {
				s_sender.shutdown();
				s_sender = null;
			}
		}
	}

	/**
	 * Get the singleton object.
	 * 
	 * @throws RGMAPermanentException
	 */
	public static StreamingSender getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_sender == null) {
				s_sender = new StreamingSender();
				s_sender.start();
			}
			return s_sender;
		}
	}

	/** If <code>true</code>, streaming sender thread is active. */
	private boolean m_active;

	/** When the next sanity check should run */
	private long m_checkTime;

	private long m_cleanupIntervalMillis;

	/** For synchronization of access to the m_registeredKeys set */
	private final Object m_keyLock = new Object();

	/** List of registered keys at last loop */
	private Set<SelectionKey> m_registeredKeys = new HashSet<SelectionKey>();

	/** Selector for multiplexed I/O. */
	private Selector m_selector;

	/**
	 * The set is added to when a new connection is made and removed when a connection is closed.
	 */
	private final Set<StreamingSource> m_sources = new HashSet<StreamingSource>();

	/** Sources to be connected ASAP by the run loop */
	private final List<StreamingSource> m_sourcesToConnect = new LinkedList<StreamingSource>();

	/** Connection (source to channel) information for unregistered (READ only) keys */
	private final List<SelectionKey> m_unregisteredKeys = new LinkedList<SelectionKey>();

	/** Status information for the inspector */
	private final Status m_status = new Status();

	/**
	 * Tuples stores to which data has been added. This is cleared when it has been processed in each loop through the
	 * run method
	 */
	private final Set<TupleStore> m_tupleStores = new HashSet<TupleStore>();

	/** Optimal packet size for NIO. */
	private final int m_optimalPacketSize;

	private SSLContext m_sslContext;

	private boolean m_allocateDirect;

	private long m_periodToKeepRedundantSourceMillis;

	private Timer m_timer;

	private StreamingSender() throws RGMAPermanentException {
		LOG = Logger.getLogger(StreamingConstants.STREAMING_SENDER_LOGGER);
		ServerConfig config = ServerConfig.getInstance();
		m_optimalPacketSize = config.getInt(ServerConstants.STREAMING_SENDER_OPTIMAL_PACKET_SIZE_BYTES);
		m_cleanupIntervalMillis = config.getLong(ServerConstants.STREAMING_SENDER_CLEANUP_INTERVAL_SECS) * 1000;
		m_checkTime = System.currentTimeMillis() + m_cleanupIntervalMillis;
		m_allocateDirect = config.getBoolean(ServerConstants.STREAMING_ALLOCATE_DIRECT);
		m_periodToKeepRedundantSourceMillis = config.getInt(ServerConstants.STREAMING_SENDER_PERIOD_TO_KEEP_REDUNDANT_SOURCE_SECS) * 1000L;
		try {
			m_selector = Selector.open();
		} catch (IOException e) {
			throw new RGMAPermanentException("Failed to create selector for streaming sender", e);
		}
		m_active = true;
		setName("StreamingSender");
		m_sslContext = RGMAContextWrapper.getInstance().getContext();
		m_timer = new Timer(true);
	}

	/**
	 * Add a new query. Note that there is no removeQuery operation. Instead, if the RunningQuery is aborted, the
	 * StreamingSource will forget about it.
	 * 
	 * @param query
	 *            RunningQuery whose results should be streamed.
	 * @throws RGMAPermanentException
	 */
	public void addQuery(RunningQuery query) throws RGMAPermanentException {
		StreamingProperties streamingProps = query.getStreamingProperties();
		StreamingSource source = getExistingSource(streamingProps);
		if (source != null) {
			if (source.addQuery(query)) {
				dataAddedToTupleStore(query.getTupleStore());
				return;
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("StreamingSource has been closed - will need to create a new one");
			}
		}

		/* If no current source - or it has been closed */
		source = new StreamingSource(streamingProps, m_optimalPacketSize, m_sslContext, m_allocateDirect,
				m_periodToKeepRedundantSourceMillis);
		synchronized (m_sources) {
			m_sources.add(source);
		}
		source.addQuery(query);
		synchronized (m_sourcesToConnect) {
			m_sourcesToConnect.add(source);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("New query - wake up thread with selector to deal with it");
		}
		m_selector.wakeup();
	}

	/**
	 * Retrieves the status info about connections.
	 * 
	 * @return A list of maps of status information for monitoring purposes "parameter name", "value"
	 */
	public List<Map<String, String>> connectionInfo() {
		List<Map<String, String>> connections = new ArrayList<Map<String, String>>();
		synchronized (m_status) {
			for (StreamingSource source : m_status.m_sources) {
				Map<String, String> map = new HashMap<String, String>();
				StreamingProperties sourceProps = source.getStreamingProperties();
				String status = "";
				if (m_status.m_registeredKeySources.contains(source)) {
					status = status + " " + "Registered";
				}
				if (m_status.m_unregisteredKeySources.contains(source)) {
					status = status + " " + "Unregistered";
				}
				if (m_status.m_sourcesToConnect.contains(source)) {
					status = status + " " + "ToConnect";
				}
				map.put("Url", String.valueOf(sourceProps.getStreamingHost()));
				map.put("Port", String.valueOf(sourceProps.getStreamingPort()));
				map.put("StreamingProtocol", String.valueOf(sourceProps.getStreamingProtocol()));
				map.put("Status", status.trim());
				connections.add(map);
			}
		}
		return connections;
	}

	/*
	 * wakeup() is only called if the TupleStore is not already in the m_tupleStores set and if the m_unregisteredKeys
	 * list is not empty. This list is added to at the top of the run loop and then elements may be removed later before
	 * it goes round again and into the select call.
	 */
	public void dataAddedToTupleStore(TupleStore store) {
		synchronized (m_tupleStores) {
			if (m_tupleStores.add(store)) {
				synchronized (m_unregisteredKeys) {
					if (!m_unregisteredKeys.isEmpty()) {
						m_selector.wakeup();
					}
				}
			}
		}
	}

	/**
	 * Main thread loop.
	 */
	@Override
	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("StreamingSender thread started");
		}
		while (m_active) {
			try {
				/* The timeout is to make sure that the cleanup thread is run */
				setStatus("Selecting");
				m_selector.select(Math.max(1, m_cleanupIntervalMillis - System.currentTimeMillis() + m_status.m_timeSinceLastCleanupMillis));

				/* Deal with selected keys */
				setStatus("Processing Keys");
				processKeys(m_selector.selectedKeys());
				synchronized (m_keyLock) {
					m_registeredKeys = m_selector.keys();
				}

				/*
				 * From this point on in the loop, entries may be removed from the m_unregisteredKeys list but nothing
				 * will be added
				 */

				/*
				 * Deal with new data added to a tuple store by seeing if any of the unregistered keys should be
				 * re-registered.
				 */
				setStatus("Dealing with unregistered keys");
				synchronized (m_tupleStores) {
					synchronized (m_unregisteredKeys) {
						Iterator<SelectionKey> iter = m_unregisteredKeys.iterator();
						while (iter.hasNext()) {
							SelectionKey key = iter.next();
							try {
								StreamingSource source = (StreamingSource) key.attachment();
								for (TupleStore t : m_tupleStores) {
									if (source.isTupleStoreUsed(t)) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("Data has been added to " + t + " so restore WRITE interest with " + source);
										}
										key.interestOps(SelectionKey.OP_WRITE + SelectionKey.OP_READ);
										iter.remove();
										break;
									}
								}
							} catch (CancelledKeyException e) {
								LOG.warn("CancelledKeyException trapped while dealing with unregistered keys " + key.attachment());
								iter.remove(); /* Make sure it doesn't come back */
							} 
						}
					}
					m_tupleStores.clear();
				}

				/*
				 * Periodically make a check that the right set of StreamingSources is known to the selector
				 */
				if (System.currentTimeMillis() > m_checkTime) {
					setStatus("Checking sources");
					/* "Re-register" any unregisteredKeys so that they are tried again */
					for (SelectionKey key : m_unregisteredKeys) {
						try {
							key.interestOps(SelectionKey.OP_WRITE + SelectionKey.OP_READ);
						} catch (CancelledKeyException e) {
							LOG.warn("CancelledKeyException trapped looping over unregistered keys " + key.attachment());
						} catch (Throwable t) {
							LOG.error("Unexpected Throwable in StreamingSender looping over unregistered keys", t);
						}
					}
					synchronized (m_unregisteredKeys) {
						m_unregisteredKeys.clear();
					}
					synchronized (m_status) {
						m_status.m_timeSinceLastCleanupMillis = System.currentTimeMillis();
					}
					m_checkTime = System.currentTimeMillis() + m_cleanupIntervalMillis;
				}

				/* Deal with new connections */
				setStatus("Dealing with new connections");
				synchronized (m_sourcesToConnect) {
					Iterator<StreamingSource> iter = m_sourcesToConnect.iterator();
					while (iter.hasNext()) {
						StreamingSource s = iter.next();
						SocketChannel c = openChannel(s);
						if (c != null) {
							try {
								c.register(m_selector, SelectionKey.OP_CONNECT, s);
								iter.remove();
								if (LOG.isDebugEnabled()) {
									LOG.debug("Registered key for new connection for " + s + " to " + c);
								}
							} catch (ClosedChannelException e) {
								LOG.warn("Error registering channel source will be left to connect later " + e.getMessage());
								closeChannel(c);
							}
						}
					}
				}

				/* Store stats for the inspector */
				setStatus("Updating status");
				synchronized (m_status) {

					synchronized (m_sources) {
						m_status.m_sources = new ArrayList<StreamingSource>(m_sources);
					}

					m_status.m_registeredKeySources = new HashSet<StreamingSource>();
					for (SelectionKey key : m_registeredKeys) {
						m_status.m_registeredKeySources.add((StreamingSource) key.attachment());
					}

					m_status.m_unregisteredKeySources = new HashSet<StreamingSource>();
					for (SelectionKey key : m_unregisteredKeys) {
						m_status.m_unregisteredKeySources.add((StreamingSource) key.attachment());
					}

					synchronized (m_sourcesToConnect) {
						m_status.m_sourcesToConnect = new HashSet<StreamingSource>(m_sourcesToConnect);
					}
				}
			} catch (ClosedSelectorException e) {
				LOG.debug("StreamingSender's Selector closed");
			} catch (IOException e) {
				LOG.error("Unexpected IOException in StreamingSender", e);
			} catch (Throwable t) {
				LOG.error("Unexpected Throwable in StreamingSender", t);
			}
		}

		try {
			m_selector.close();
		} catch (IOException e) {
			LOG.warn("Error shutting down streaming sender: " + e);
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("StreamingSender closing down");
		}
	}

	/**
	 * Retrieves the status info for this streaming sender.
	 * 
	 * @return A map of status information for monitoring purposes "parameter name", "value"
	 */
	public Map<String, String> statusInfo() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("Active", String.valueOf(m_active));
		map.put("WriteBufferSizeBytes", String.valueOf(m_optimalPacketSize));
		map.put("CleanupIntervalMillis", String.valueOf(m_cleanupIntervalMillis));
		synchronized (m_status) {
			map.put("SourcesCount", String.valueOf(m_status.m_sources.size()));
			map.put("RegisteredKeysCount", String.valueOf(m_status.m_registeredKeySources.size()));
			map.put("UnregisteredKeysCount", String.valueOf(m_status.m_unregisteredKeySources.size()));
			map.put("SourcesToConnectCount", String.valueOf(m_status.m_sourcesToConnect.size()));
			map.put("TimeSinceLastCleanupMillis", String.valueOf(System.currentTimeMillis() - m_status.m_timeSinceLastCleanupMillis));
			map.put("Status", m_status.m_status);
		}
		return map;
	}

	/**
	 * Closes the channel (including input, output and socket).
	 * 
	 * @param channel
	 *            Channel to close.
	 */
	private void closeChannel(SocketChannel channel) {
		/*
		 * Shutdown input and output to make sure that all sockets are closed properly. Without this, Linux can leave
		 * socket in CLOSE_WAIT state as child processes (other threads) have a file descriptor for the socket.
		 */
		try {
			channel.socket().shutdownInput();
		} catch (IOException e) {
			LOG.warn("Error trying to shut down input: " + e.getMessage());
		}

		try {
			channel.socket().shutdownOutput();
		} catch (IOException e) {
			LOG.warn("Error trying to shut down output: " + e.getMessage());
		}

		try {
			channel.socket().close();
		} catch (IOException e) {
			LOG.warn("Error trying to close socket: " + e.getMessage());
		}

		try {
			channel.close();
		} catch (IOException e) {
			LOG.warn("Error trying to close socket channel: " + e.getMessage());
		}
		m_selector.wakeup();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Closed channel and woke up main thread");
		}
	}

	/**
	 * Get the StreamingSource object for a specified streaming endpoint and streaming protocol.
	 * 
	 * @param streamingProps
	 *            StreamingProperties object containing streaming endpoint and protocol.
	 * @return A StreamingSource object with the corresponding endpoint and protocol, or <code>null</code> if no
	 *         corresponding StreamingSource is available.
	 */
	private StreamingSource getExistingSource(StreamingProperties streamingProps) {
		synchronized (m_sources) {
			for (StreamingSource source : m_sources) {
				StreamingProperties sourceProps = source.getStreamingProperties();
				if (sourceProps.getStreamingHost().equals(streamingProps.getStreamingHost())
						&& sourceProps.getStreamingPort() == streamingProps.getStreamingPort()
						&& sourceProps.getStreamingProtocol() == streamingProps.getStreamingProtocol()) {
					return source;
				}
			}
		}
		return null;
	}

	/**
	 * Create a new channel for a StreamingSource.
	 * 
	 * @param source
	 *            StreamingSource object to open a connection for.
	 * @return SocketChannel for the connection, with an associated StreamingSource object. <code>query</code> is
	 *         automatically added to the StreamingSource object.
	 */
	private SocketChannel openChannel(StreamingSource source) {
		StreamingProperties streamingProps = source.getStreamingProperties();
		InetSocketAddress address = new InetSocketAddress(streamingProps.getStreamingHost(), streamingProps.getStreamingPort());
		SocketChannel channel = null;
		try {
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			channel.connect(address);
			return channel;
		} catch (IOException e) {
			if (channel != null) {
				closeChannel(channel);
				LOG.warn("IOException " + e.getMessage() + " No connection created");
			}
		} catch (SecurityException e) {
			if (channel != null) {
				closeChannel(channel);
				LOG.warn("SecurityException " + e.getMessage() + " No connection created");
			}
		}
		return null;
	}

	protected void clearKey(SelectionKey key) throws RGMAPermanentException {
		closeChannel((SocketChannel) key.channel());
		resetSource((StreamingSource) key.attachment());
	}

	/**
	 * Deal with selected keys
	 * 
	 * @throws RGMAPermanentException
	 */
	private void processKeys(Set<SelectionKey> selectedKeys) throws RGMAPermanentException {
		try {
			for (SelectionKey key : selectedKeys) {
				try {
					SocketChannel c = (SocketChannel) key.channel();
					if (key.isConnectable()) {
						try {
							if (c.finishConnect()) {
								key.interestOps(SelectionKey.OP_WRITE + SelectionKey.OP_READ);
								if (LOG.isDebugEnabled()) {
									LOG.debug("Channel connected for " + key.attachment());
								}
							}
						} catch (IOException e) {
							LOG.warn("Channel connection failed - will reopen " + key.attachment() + " " + e.getMessage());
							clearKey(key);
							/* The key is no longer usable as the channel has been closed */
							continue;
						}
					}
					if (key.isWritable()) {
						writeData(key);
					}
					if (key.isReadable()) {
						readData(key);
					}
					if (LOG.isDebugEnabled()) {
						int ki = key.interestOps();
						LOG.debug(key.attachment() + " has interest" + (((ki & SelectionKey.OP_READ) != 0) ? " READ" : "")
								+ (((ki & SelectionKey.OP_WRITE) != 0) ? " WRITE" : "") + (((ki & SelectionKey.OP_CONNECT) != 0) ? " CONNECT" : "")
								+ (((ki & SelectionKey.OP_ACCEPT) != 0) ? " ACCEPT" : "") + " before going to select call");
					}
				} catch (CancelledKeyException e) {
					LOG.warn("CancelledKeyException trapped in process keys " + key.attachment());
					break;
				}
			}
		} finally {
			selectedKeys.clear();
		}
	}
	
	private class SourceConnectionTask extends TimerTask {

		private StreamingSource m_source;

		public SourceConnectionTask(StreamingSource source) {
			m_source = source;
		}

		@Override
		public void run() {
			synchronized (m_sourcesToConnect) {
				m_sourcesToConnect.add(m_source);
			}		
		}
	}
	
	/**
	 * Reset the source and add it to the m_sourcesToConnect list after some fixed time
	 * 
	 * TODO avoid the fixed number
	 */
	private void resetSource(StreamingSource source) throws RGMAPermanentException {
		source.reset();
		m_timer.schedule(new SourceConnectionTask(source), 300000);
	}

	private void setStatus(String msg) {
		synchronized (m_status) {
			m_status.m_status = msg;
		}
	}

	/**
	 * Shut down the streaming sender thread. No interrupt is necessary as the selector notices!
	 */
	private void shutdown() {
		m_active = false;
		try {
			m_selector.close();
		} catch (IOException e) {
			LOG.warn("Error shutting down streaming sender: " + e);
		}
		m_timer.cancel();
	}

	/**
	 * Write data to a streaming connection.
	 * 
	 * @param unregisteredKeys
	 * @return Status of the connection.
	 * @throws RGMAPermanentException
	 */
	private void writeData(SelectionKey key) throws RGMAPermanentException {
		SocketChannel channel = (SocketChannel) key.channel();
		if (!channel.isOpen()) {
			// Channel could be closed, but not yet deregistered.
			LOG.warn("writeData called on closed channel");
			return;
		}
		StreamingSource source = (StreamingSource) key.attachment();
		ByteBuffer encryptedWriteBuffer = source.popBytes();
		if (encryptedWriteBuffer != null) {
			if (encryptedWriteBuffer.position() > 0) {
				int bytesWritten = 0;
				try {
					encryptedWriteBuffer.flip();
					bytesWritten = channel.write(encryptedWriteBuffer);
					if (source.getHandshakeStatus() == HandshakeStatus.NEED_UNWRAP && encryptedWriteBuffer.remaining() == 0) {
						key.interestOps(SelectionKey.OP_READ);
						synchronized (m_unregisteredKeys) {
							m_unregisteredKeys.add(key);
						}
						if (LOG.isDebugEnabled()) {
							LOG.debug("Key with " + source + " NEEDS_UNWRAP and all date sent so set to READ interest only");
						}
					}
					encryptedWriteBuffer.compact();
				} catch (IOException e) {
					closeChannel(channel);
					resetSource(source);
					LOG.warn("Error writing to channel so closed it and made a new one for " + source + " " + e.getMessage());
					return;
				}
				if (bytesWritten > 0) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(bytesWritten + " bytes written to channel from " + source);
					}
				} else {
					/* Re-open connection for streaming source */
					closeChannel(channel);
					resetSource(source);
					LOG.warn("Data available but no bytes written, so closed channel and made a new one");
				}
			} else {
				key.interestOps(SelectionKey.OP_READ);
				synchronized (m_unregisteredKeys) {
					m_unregisteredKeys.add(key);
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug("No data written for key with " + key.attachment() + " set to READ interest only");
				}
			}
		} else {
			/*
			 * popBytes has returned null to indicate that the StreamingSource is no longer needed
			 */
			closeChannel(channel);
			synchronized (m_sources) {
				if (!m_sources.remove(source)) {
					LOG.error(source + " not present in list of sources");
				}
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Reached the end of the streaming source, closed channel");
			}
		}
	}
}
