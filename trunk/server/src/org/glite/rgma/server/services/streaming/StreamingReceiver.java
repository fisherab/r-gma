/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.streaming;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.naming.ConfigurationException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.consumer.Consumable;
import org.glite.rgma.server.services.consumer.ConsumerService;
import org.glite.rgma.server.services.consumer.RunningReply;
import org.glite.rgma.server.services.mediator.PlanEntry;
import org.glite.rgma.server.services.mediator.ProducerDetails;
import org.glite.rgma.server.services.producer.secondary.SecondaryProducerService;
import org.glite.rgma.server.servlets.RGMAContextWrapper;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.UnknownResourceException;

/**
 * Receives tuples on behalf of Consumer and Secondary Producer resources. Implemented as a singleton to ensure that
 * only one can be created. It is possible to have more than one instance when a web application is being reloaded as
 * different classloaders may be used. For this reason it is essential that the streaming server is shut down and
 * releases its server socket when the web application is shut down.
 * <p>
 * Consumers add RunningReply objects to the streaming receiver to indicate the queries they are expecting replies to.
 * The StreamingReceiver keeps a record of the IP addresses of the producers it expects connections from and does not
 * accept any connections from other hosts. This is intended as a basic security precaution but does not provide
 * complete protection from malicious users who have access to an R-GMA server.
 * <p>
 * For each connection accepted, a StreamingSink object is created to handle the data received. This object is
 * responsible for identifying the streaming protocol, decoding the result sets and passing them on to the appropriate
 * consumer via the RunningReply object.
 */
public class StreamingReceiver extends StreamingEndpoint {
	/**
	 * A Timer Task used to keep an eye on the TaskInvocator threads
	 */
	private class CleanoutRunningReplies extends TimerTask {

		/**
		 * Periodically check m_replies for consumers/secondary producers that are no longer there
		 */
		@Override
		public void run() {
			try {
				/*
				 * The ping calls below can cause a consumer or secondary producer to be destroyed - this calls
				 * removeReply which is able to modify m_replies as it is in the same thread causing a concurrent
				 * modification exception. So we create a list of replies to remove and iterate over a copy of the list.
				 * Note also that the m_replies must not be locked while ping is called - to avoid deadlocks.
				 */
				ArrayList<RunningReply> toCheck = null;
				synchronized (m_replies) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Checking " + m_replies.size() + "  replies - looking for stale ones");
					}
					toCheck = new ArrayList<RunningReply>(m_replies);
				}
				ArrayList<RunningReply> toRemove = new ArrayList<RunningReply>();
				for (RunningReply reply : toCheck) {
					Consumable consumable = reply.getConsumable();
					if (!reply.isActive()) {
						// Don't remove it from the plan or it may return too soon
						toRemove.add(reply);
						LOG.info("Removing inactive reply " + reply);
					} else {
						try {
							int resourceId = consumable.getEndpoint().getResourceID();
							if (consumable.getEndpoint().getURL().getPath().contains(ServerConstants.CONSUMER_SERVICE_NAME)) {
								m_cservice.ping(resourceId);
							} else {
								m_spservice.ping(resourceId);
							}
						} catch (UnknownResourceException e) {
							toRemove.add(reply);
							LOG.warn("Could not find resource. Will remove " + reply);
						}
					}
				}
				synchronized (m_replies) {
					m_replies.removeAll(toRemove);
				}
				synchronized (m_status) {
					m_status.m_timeSinceLastCleanupMillis = System.currentTimeMillis();
				}
			} catch (Exception e) {
				LOG.error("Unexpected exception", e);
			}
		}
	}

	private class Status {

		public long m_timeSinceLastCleanupMillis;

		private Status() {
			m_timeSinceLastCleanupMillis = System.currentTimeMillis();
		}

	}

	/** Whether the streaming receiver is active. */
	private static boolean s_active;

	/** A timer to interrupt a taskInvocation thread */
	private static Timer s_cleanoutRunningRepliesTimer;

	private static final Object s_instanceLock = new Object();

	/** Singleton instance */
	private static StreamingReceiver s_receiver;

	/**
	 * Stop the streaming receiver main thread and disconnect from the server socket.
	 */
	public static void dropInstance() {
		synchronized (s_instanceLock) {
			if (s_receiver != null) {
				s_active = false;
				s_receiver.interrupt();
				s_receiver = null;
			}
			if (s_cleanoutRunningRepliesTimer != null) {
				s_cleanoutRunningRepliesTimer.cancel();
			}
		}
	}

	/**
	 * Get the singleton object.
	 * 
	 * @throws RGMAPermanentException
	 */
	public static StreamingReceiver getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_receiver == null) {
				ServerConfig config = ServerConfig.getInstance();
				s_receiver = new StreamingReceiver(config.getInt(ServerConstants.STREAMING_RECEIVER_PORT));
				s_active = true;
				s_receiver.start();
			}
			return s_receiver;
		}
	}

	private TimerTask m_cleanout;

	private long m_cleanupIntervalMillis;

	private ConsumerService m_cservice;

	/** Number of active connections. */
	private Integer m_numConnections;

	/** Port number the streaming receiver listens on. */
	private final int m_port;

	private List<RunningReply> m_replies;

	/** Selector for multiplexed I/O. */
	private Selector m_selector;

	/** Count of registered keys (channels). */
	private Integer m_selectorCount = 0;

	/** Socket channel of this server. */
	private ServerSocketChannel m_serverChannel;

	private ServerSocket m_serverSocket = null;

	private SecondaryProducerService m_spservice;

	/** Status for the inspector */
	private final Status m_status = new Status();

	/** IP addresses of hosts that are allowed to contact the streaming server. */
	/*
	 * FIXME The list of valid hosts is added to but never cleaned out potentially this could be a memory leak in
	 * practice this is unlikely as at present the lcgproduction grid has approx 250 sites/mon boxes so even if the
	 * number of mon boxes doubles or trebles it is unlikely to cause any memory problems
	 */
	private Set<String> m_validHosts;

	private SSLContext m_sslContext;

	private boolean m_allocateDirect;

	/**
	 * Private constructor to prevent instances other than the singleton being created.
	 * 
	 * @param port
	 *            Port number for the streaming receiver to listen on.
	 * @throws ConfigurationException
	 *             If the streaming server could not be started on this port.
	 * @throws RGMAPermanentException
	 */
	private StreamingReceiver(int port) throws RGMAPermanentException {
		LOG = Logger.getLogger(StreamingConstants.STREAMING_RECEIVER_LOGGER);
		ServerConfig config = ServerConfig.getInstance();
		m_cleanupIntervalMillis = config.getLong(ServerConstants.STREAMING_RECEIVER_CLEANUP_INTERVAL_SECS) * 1000;
		m_port = port;
		m_numConnections = 0;
		m_replies = new LinkedList<RunningReply>();
		m_validHosts = new HashSet<String>();
		m_allocateDirect = config.getBoolean(ServerConstants.STREAMING_ALLOCATE_DIRECT);
		setName("StreamingReceiver");
		try {
			m_serverChannel = ServerSocketChannel.open();
			m_serverSocket = m_serverChannel.socket();

			// It has been seen that the streaming receiver port is not always
			// freed up immediately when the servlet container is restarted
			boolean bound = false;
			int attempts = 0;
			while (bound == false & attempts < 60) {
				LOG.debug("Attempting to bind to port " + m_port + " attempt " + attempts);
				bound = bind();
				attempts++;
				try {
					sleep(1000);
				} catch (InterruptedException e) {}
			}

			if (bound == false) {
				throw new RGMAPermanentException("Could not start streaming receiver on port " + m_port);
			}

			if (attempts > 1) {
				LOG.warn("Streaming receiver port " + m_port + " did not open on the first attempt");
			}

			m_serverChannel.configureBlocking(false);
			m_selector = Selector.open();
			m_serverChannel.register(m_selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			throw new RGMAPermanentException("Could not start streaming receiver on port " + m_port, e);
		}

		m_cservice = null;
		m_spservice = null;
		m_cleanout = null;

		m_sslContext = RGMAContextWrapper.getInstance().getContext();

		LOG.debug("StreamingReceiver Started");
	}

	/**
	 * Add a running reply to the streaming receiver. This represents a query being run by a Consumer or Secondary
	 * Producer on a particular producer. Tuples that are received for this particular query will be pushed to the
	 * consumer via the RunningReply object.
	 * 
	 * @param reply
	 *            RunningReply object.
	 */
	public void addReply(RunningReply reply) {
		PlanEntry planEntry = reply.getPlanEntry();
		synchronized (m_replies) {
			m_replies.add(reply);
		}
		ResourceEndpoint producerEndpoint = planEntry.getProducer().getEndpoint();
		try {
			String ipAddress = InetAddress.getByName(producerEndpoint.getURL().getHost()).getHostAddress();
			m_validHosts.add(ipAddress);
		} catch (UnknownHostException e) {
			LOG.warn("Host name of producer endpoint unknown: " + producerEndpoint.getURL().getHost());
		}
	}

	/**
	 * Get the port number the streaming receiver is listening on
	 */
	public int getPort() {
		return m_port;
	}

	/**
	 * Remove a running reply from the streaming receiver. No further tuples will be pushed to the reply.
	 */
	public void removeReply(RunningReply reply) {
		synchronized (m_replies) {
			m_replies.remove(reply);
		}
	}

	/**
	 * Retrieves the status info about running replies.
	 * 
	 * @return A list of maps of status information for monitoring purposes "parameter name", "value"
	 */
	public List<Map<String, String>> replyInfo(int maxEntries) {
		List<Map<String, String>> replies = new ArrayList<Map<String, String>>();

		synchronized (m_replies) {
			int n = 0;
			for (RunningReply reply : m_replies) {
				Map<String, String> map = new HashMap<String, String>();
				map.put("ConsumerID", String.valueOf(reply.getConsumable().getEndpoint().getResourceID()));

				if (reply instanceof RunningReply) {
					ProducerDetails producer = reply.getPlanEntry().getProducer();
					map.put("ProducerURL", String.valueOf(producer.getEndpoint().getURL()));
					map.put("ProducerID", String.valueOf(producer.getEndpoint().getResourceID()));
					if (producer.getTables() == null) {
						map.put("Table", "Not known - directed query");
					} else {
						map.put("Table", String.valueOf(producer.getTables().keySet().toArray()[0]));
					}
				} else {
					map.put("ProducerURL", "None");
					map.put("ProducerID", "None");
					map.put("Table", "None");
				}
				replies.add(map);
				if (++n >= maxEntries)
					break;
			}
		}
		return replies;
	}

	/**
	 * Main thread loop.
	 */
	@Override
	public void run() {
		while (s_active) {
			try {
				m_selector.select();
				synchronized (m_selectorCount) {
					m_selectorCount = m_selector.keys().size();
				}
				Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
				try {
					for (SelectionKey key : selectedKeys) {
						if (key.isAcceptable()) {
							acceptConnection(key);
						}
						if (key.isReadable()) {
							readData(key);
						}
						if (key.isWritable()) {
							writeData(key);
						}
						if (LOG.isDebugEnabled()) {
							int ki = key.interestOps();
							LOG.debug(key.attachment() + " has interest" + (((ki & SelectionKey.OP_READ) != 0) ? " READ" : "")
									+ (((ki & SelectionKey.OP_WRITE) != 0) ? " WRITE" : "") + (((ki & SelectionKey.OP_CONNECT) != 0) ? " CONNECT" : "")
									+ (((ki & SelectionKey.OP_ACCEPT) != 0) ? " ACCEPT" : "") + " before going to select call");
						}
					}
				} catch (CancelledKeyException e) {
					LOG.warn("CancelledKeyException trapped");
				} finally {
					selectedKeys.clear();
				}

			} catch (Throwable t) {
				LOG.error("Unexpected error in StreamingReceiver ", t);
			}
		}
		try {
			m_selector.close();
			m_serverChannel.close();
			m_serverSocket.close();
		} catch (IOException e) {
			LOG.warn("Error shutting down streaming receiver " + e.getMessage());
		}
	}

	public void setConsumer(ConsumerService consumer) {
		m_cservice = consumer;
		startTimer();
	}

	public void setSecondaryProducer(SecondaryProducerService sp) {
		m_spservice = sp;
		startTimer();
	}

	/**
	 * Retrieves the status info for this streaming receiver.
	 * 
	 * @return A map of status information for monitoring purposes "parameter name", "value"
	 */
	public Map<String, String> statusInfo() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("Active", String.valueOf(s_active));
		map.put("Port", String.valueOf(m_port));
		synchronized (m_replies) {
			map.put("RunningRepliesCount", String.valueOf(m_replies.size()));
		}
		synchronized (m_numConnections) {
			map.put("ConnectionCount", String.valueOf(m_numConnections));
		}
		synchronized (m_selectorCount) {
			map.put("SelectorKeyCount", String.valueOf(m_selectorCount));
		}
		map.put("CleanupIntervalMillis", String.valueOf(m_cleanupIntervalMillis));
		synchronized (m_status) {
			map.put("TimeSinceLastCleanupMillis", String.valueOf(System.currentTimeMillis() - m_status.m_timeSinceLastCleanupMillis));
		}
		return map;
	}

	/**
	 * Accepts a new connection.
	 * 
	 * @param key
	 *            Key to accept.
	 * @throws IOException
	 *             If accept or channel registration fails.
	 * @throws SocketException
	 *             If keep-alive can't be set.
	 */
	private void acceptConnection(SelectionKey key) throws IOException, SocketException, RGMAPermanentException {
		ServerSocketChannel server = (ServerSocketChannel) key.channel();
		SocketChannel channel = server.accept();
		Socket socket = channel.socket();
		/*
		 * Enable keepAlive. This sends a packet every (by default) 2 hours to make sure that the connection is open. It
		 * also ensures the socket closes after at most 9 (probes) 75s (interval) ~ 11 minutes.
		 */
		socket.setKeepAlive(true);

		String remoteHostAddress = socket.getInetAddress().getHostAddress();
		synchronized (m_numConnections) {
			m_numConnections++;
		}
		if (m_validHosts.contains(remoteHostAddress)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("New connection accepted from: " + socket.getInetAddress() + " Total: " + m_numConnections);
			}
			channel.configureBlocking(false);
			/*
			 * No need to synchronize on m_replies - but the StreamingSink must do so when it uses it
			 */
			key = channel.register(m_selector, SelectionKey.OP_READ, new StreamingSink(m_replies, m_sslContext, m_allocateDirect));
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Unexpected IP address " + remoteHostAddress + " contacted streaming receiver - closing channel.");
			}
			closeChannel(channel);
		}
	}

	/**
	 * Attempts to bind m_port to m_serverSocket.
	 * 
	 * @return A boolean, true if successful, false if a BindException
	 * @throws IOException
	 */
	private boolean bind() throws IOException {
		try {
			m_serverSocket.bind(new InetSocketAddress(m_port));
		} catch (BindException e) {
			return false;
		}
		return true;
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
			LOG.debug("Error trying to shut down input: " + e.getMessage());
		}

		try {
			channel.socket().shutdownOutput();
		} catch (IOException e) {
			LOG.debug("Error trying to shut down output: " + e.getMessage());
		}

		try {
			channel.socket().close();
		} catch (IOException e) {
			LOG.debug("Error trying to close socket: " + e.getMessage());
		}

		try {
			channel.close();
		} catch (IOException e) {
			LOG.debug("Error trying to close socket channel: " + e.getMessage());
		}
		m_selector.wakeup();

		/*
		 * NB: the channel may not close immediately, as the key is just added to the cancelled key list for later
		 * processing, so don't worry about seemingly extraneous calls to close the channel.
		 */
		synchronized (m_numConnections) {
			m_numConnections--;
		}
	}

	private void writeData(SelectionKey key) throws RGMAPermanentException {
		SocketChannel channel = (SocketChannel) key.channel();
		if (!channel.isOpen()) {
			// Channel could be closed, but not yet deregistered.
			LOG.warn("writeData called on closed channel");
			return;
		}
		StreamingSink sink = (StreamingSink) key.attachment();
		ByteBuffer encryptedWriteBuffer = sink.popBytes();
		if (encryptedWriteBuffer.position() > 0) {
			int bytesWritten = 0;
			try {
				encryptedWriteBuffer.flip();
				bytesWritten = channel.write(encryptedWriteBuffer);
				if (sink.getHandshakeStatus() == HandshakeStatus.NEED_UNWRAP && encryptedWriteBuffer.remaining() == 0) {
					key.interestOps(SelectionKey.OP_READ);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Key with " + sink + " NEEDS_UNWRAP and all date sent so set to READ interest only");
					}
				}
				encryptedWriteBuffer.compact();
			} catch (IOException e) {
				clearKey(key);
				LOG.warn("Error writing to channel so closed it " + sink + " " + e.getMessage());
				return;
			}
			if (bytesWritten > 0) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(bytesWritten + " bytes written to channel from " + sink);
				}
			} else {
				/* Re-open connection for streaming source */
				clearKey(key);
				LOG.warn("Data available but no bytes written, so closed channel");
			}
		} else {
			key.interestOps(SelectionKey.OP_READ);
			if (LOG.isDebugEnabled()) {
				LOG.debug("No data written for key with " + key.attachment() + " set to READ interest only");
			}
		}
	}

	synchronized void startTimer() {
		if (m_cservice != null & m_spservice != null & m_cleanout == null) {
			m_cleanout = new CleanoutRunningReplies();
			s_cleanoutRunningRepliesTimer = new Timer("StreamingReceiverCleanup", true);
			s_cleanoutRunningRepliesTimer.schedule(m_cleanout, m_cleanupIntervalMillis, m_cleanupIntervalMillis);
			if (LOG.isInfoEnabled()) {
				LOG.info("Created CleanoutRunningReplies timer with a repeat period of " + m_cleanupIntervalMillis / 1000 + " seconds");
			}
		}
	}

	@Override
	protected void clearKey(SelectionKey key) throws RGMAPermanentException {
		closeChannel((SocketChannel) key.channel());
	}
}
