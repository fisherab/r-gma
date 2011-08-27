/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.consumer;

import java.util.Observable;

import org.apache.log4j.Logger;
import org.glite.rgma.server.remote.RemoteProducer;
import org.glite.rgma.server.remote.RemoteResourceBase;
import org.glite.rgma.server.services.mediator.PlanEntry;
import org.glite.rgma.server.services.mediator.ProducerDetails;
import org.glite.rgma.server.services.streaming.StreamingConstants;
import org.glite.rgma.server.services.tasks.Task;
import org.glite.rgma.server.services.tasks.TaskManager;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;

/**
 * Handles tuples sent by a specific producer in response to a query. All tuples sent back by the corresponding producer
 * pass through this object.
 */
public class RunningReply extends Observable {
	private static final Logger LOG = Logger.getLogger(StreamingConstants.STREAMING_RECEIVER_LOGGER);

	/** If true, start command has been sent to the producer and streamed tuples will be accepted */
	private boolean m_active;

	/** Consumable (consumer or secondary producer that owns this reply */
	private final Consumable m_consumer;

	private UserContext m_context;

	private int m_maximumTaskAttemptCount;

	private long m_maximumTaskTimeMillis;

	/** Number of tuples which have been received by this reply */
	private int m_numTuplesReceived;

	/** Entry from the consumer's plan which this reply corresponds to */
	private final PlanEntry m_planEntry;

	/** Holds the host name of the producer being contacted */
	private String m_producerServiceURL;

	/** Reference to the 'start' message for this reply, of <code>null</code> if no start sent */
	private Task m_start;

	/** Queue for asynchrous messaging */
	private final TaskManager m_taskInvocationQueue;

	/** Holds the Id used for identifying tasks handed to the task manager as belonging to this class. */
	private final String m_taskOwnerId = getClass().getName();

	/** The name of the table being processed. This is only set for a secondary producer for a consumer it is null. */
	private String m_vdbTableName;

	public long m_intervalToGiveUpOnUnreachableMillis;

	public Long m_timeToGiveUpOnUnreachable = 0L;

	public RunningReply(PlanEntry planEntry, Consumable consumer, TaskManager taskInvocationQueue, long maximumTaskTimeMillis, int maximumTaskAttemptCount,
			long intervalToGiveUpOnUnreachableMillis, String vdbTableName) {
		m_planEntry = planEntry;
		m_producerServiceURL = planEntry.getProducer().getEndpoint().getURL().toString();
		m_consumer = consumer;
		m_taskInvocationQueue = taskInvocationQueue;
		m_maximumTaskTimeMillis = maximumTaskTimeMillis;
		m_maximumTaskAttemptCount = maximumTaskAttemptCount;
		m_vdbTableName = vdbTableName;
		m_intervalToGiveUpOnUnreachableMillis = intervalToGiveUpOnUnreachableMillis;
	}

	/**
	 * Stop executing the plan entry. Sends abort message to the producer and sets a flag to reject any further results.
	 * This does not consider plan consistency as it is called when consumables are being closed or when plans are being
	 * modified.
	 */
	public void abort() {
		synchronized (this) { // Do not send at same time as start
			m_active = false;
			// Only send abort if start has been sent
			if (m_start != null) {
				m_taskInvocationQueue.add(new Abort());
			}
		}
	}

	/** Get the consumer that created this reply. */
	public Consumable getConsumable() {
		return m_consumer;
	}

	/** Get the plan entry associated with this reply. */
	public PlanEntry getPlanEntry() {
		return m_planEntry;
	}

	/**
	 * Returns <code>true</code> if this reply is still accepting tuples from the producer.
	 */
	public boolean isActive() {
		return m_active;
	}

	/**
	 * Test if this reply matches the specified producer and query details. This is used by a StreamingSink to determine
	 * which RunningReply to pass a set of tuples on to.
	 */
	public boolean matches(ResourceEndpoint sourceProducer, ResourceEndpoint targetConsumer, String query) {
		if (m_planEntry.getProducer().getEndpoint().equals(sourceProducer) && m_planEntry.getSelect().toString().equals(query)
				&& m_consumer.getEndpoint().equals(targetConsumer)) {
			return true;
		}

		return false;
	}

	/**
	 * Receive a set of results from a StreamingSink. Records the number of tuples received and attempts to push the
	 * results onto the consumable (consumer or secondary ptoducer). If this reply is not active, the results are
	 * rejected. Don't remove from the plan as we don't want it coming back again.
	 */
	public void push(TupleSet tuples) {
		synchronized (this) {
			if (m_active) {
				m_consumer.push(tuples, m_vdbTableName);
				m_numTuplesReceived += tuples.size();
				if (tuples.isEndOfResults()) {
					m_active = false;
				}
			} else {
				LOG.debug("Dropping " + tuples.size() + " tuples for inactive consumer reply");
			}
		}
	}

	/**
	 * Execute the plan entry. Sends start message to the producer.
	 */
	public void start(TimeInterval timeout, QueryProperties queryProperties, StreamingProperties streamingProperties, UserContext context) {
		synchronized (this) { // Do not send at same time as abort
			m_active = true;
			m_start = new Start(timeout, queryProperties, streamingProperties, context);
			m_taskInvocationQueue.add(m_start);
		}
	}

	/**
	 * Test if the producer is still alive. If the producer is found to be dead, a removeProducer message will be sent
	 * to the Consumable.
	 */
	public void testProducer() {
		m_taskInvocationQueue.add(new Ping());
	}

	@Override
	public String toString() {
		ProducerDetails p = getPlanEntry().getProducer();
		return p.toString() + " -> " + m_consumer;
	}

	/**
	 * Task which instructs the producer to stop executing the query.
	 */
	private class Abort extends Task {
		public Abort() {
			super(m_taskOwnerId, m_producerServiceURL, m_maximumTaskTimeMillis, m_maximumTaskAttemptCount);
		}

		@Override
		public Result invoke() {
			ResourceEndpoint endpoint = m_planEntry.getProducer().getEndpoint();
			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Sending abort to " + endpoint);
				}
				RemoteProducer.abort(endpoint.getURL(), endpoint.getResourceID(), m_consumer.getEndpoint());
			} catch (RemoteException e) {
				LOG.warn("Error sending abort request to " + endpoint);
				return Result.SOFT_ERROR;
			} catch (UnknownResourceException e) {
				// No problem - producer is dead anyway
			} catch (RGMAException e) {
				LOG.error("Unexpected error in abort to " + m_planEntry, e);
				return Result.HARD_ERROR;
			}
			return Result.SUCCESS;
		}
	}

	/**
	 * Task which tests if the producer is still alive.
	 */
	private class Ping extends Task {

		public Ping() {
			super(m_taskOwnerId, m_producerServiceURL, m_maximumTaskTimeMillis, m_maximumTaskAttemptCount);
		}

		@Override
		public Result invoke() {
			ResourceEndpoint endpoint = m_planEntry.getProducer().getEndpoint();
			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Sending ping to " + endpoint);
				}
				RemoteResourceBase.ping(endpoint.getURL(), endpoint.getResourceID());
				synchronized (m_timeToGiveUpOnUnreachable) {
					m_timeToGiveUpOnUnreachable = 0L;
				}
				return Result.SUCCESS;
			} catch (RemoteException e) {
				synchronized (m_timeToGiveUpOnUnreachable) {
				if (m_timeToGiveUpOnUnreachable == 0L) {
					m_timeToGiveUpOnUnreachable = System.currentTimeMillis() + m_intervalToGiveUpOnUnreachableMillis;
					LOG.warn("RemoteException " + e.getMessage() + " sending ping request to " + endpoint + " time noted to see if permanent");
					return Result.SOFT_ERROR;
				} else if (System.currentTimeMillis() > m_timeToGiveUpOnUnreachable) {
					LOG.warn("RemoteException " + e.getMessage() + " sending ping request to " + endpoint + " appears to be permanent - remove it from plan.");
					m_consumer.removeProducer(m_planEntry.getProducer().getEndpoint());
					m_active = false;
					return Result.HARD_ERROR;
				} else {
					LOG.warn("RemoteException " + e.getMessage() + " sending ping request to " + endpoint + " again");
					return Result.SOFT_ERROR;
				}}
			} catch (RGMAPermanentException e) {
				LOG.error("RGMAPermanentException " + e.getFlattenedMessage() + " sending ping request to " + endpoint);
				return Result.HARD_ERROR;
			} catch (UnknownResourceException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(endpoint + " has died");
				}
				m_consumer.removeProducer(m_planEntry.getProducer().getEndpoint());
				m_active = false;
				return Result.SUCCESS;
			} catch (RGMATemporaryException e) {
				LOG.error("RGMAPermanentException " + e.getFlattenedMessage() + " sending ping request to " + endpoint);
				return Result.SOFT_ERROR;
			}

		}
	}

	/**
	 * Task which instructs the producer to execute the query.
	 */
	private class Start extends Task {
		/** Query properties */
		private final QueryProperties m_queryProperties;

		/** Streaming properties */
		private StreamingProperties m_streamingProperties;

		/** Query timeout */
		private final TimeInterval m_timeout;

		public Start(TimeInterval timeout, QueryProperties queryProperties, StreamingProperties streamingProperties, UserContext context) {
			super(m_taskOwnerId, m_producerServiceURL, m_maximumTaskTimeMillis, m_maximumTaskAttemptCount);
			m_timeout = timeout;
			m_queryProperties = queryProperties;
			m_streamingProperties = streamingProperties;
			m_context = context;
		}

		@Override
		public Result invoke() {
			Result result = Result.SUCCESS;
			ResourceEndpoint endpoint = m_planEntry.getProducer().getEndpoint();

			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Sending start to " + m_planEntry);
				}
				TupleSet timestamps = RemoteProducer.start(endpoint.getURL(), endpoint.getResourceID(), m_planEntry.getSelect().toString(), m_queryProperties,
						m_timeout, m_consumer.getEndpoint(), m_streamingProperties, m_context);
				setChanged();
				notifyObservers(timestamps);
			} catch (RemoteException e) {
				LOG.debug("Error sending start request to " + endpoint + " " + e.getMessage());
				result = Result.SOFT_ERROR;
			} catch (UnknownResourceException e) {
				LOG.debug("UnknownResourceException sending start request to " + endpoint);
				m_consumer.removeProducer(m_planEntry.getProducer().getEndpoint());
				m_active = false;
			} catch (NumericException e) {
				/*
				 * This is thrown by the start call if, despite the checks made on the query by the consumable, it fails
				 * because of some RDBMS specific feature.
				 */
				LOG.debug("NumericException in start to " + m_planEntry + " " + e.getMessage() + " code: " + e.getNumber());
				m_consumer.abend(new RGMAPermanentException(e.getMessage()));
				result = Result.HARD_ERROR;
			} catch (RGMAException e) {
				LOG.error("Unexpected RGMAException in start to " + m_planEntry + " " + e.getFlattenedMessage());
				m_consumer.removeProducer(m_planEntry.getProducer().getEndpoint());
				m_active = false;
				result = Result.HARD_ERROR;
			} catch (Throwable t) {
				LOG.error("Unexpected Throwable in start to " + m_planEntry, t);
				m_consumer.removeProducer(m_planEntry.getProducer().getEndpoint());
				m_active = false;
				result = Result.HARD_ERROR;
			}

			return result;
		}
	}
}
