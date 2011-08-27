/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.naming.ConfigurationException;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.tasks.Task.Result;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;

/**
 * The TaskManager has all public methods synchronized. The TaskInvocationMonitor also synchronizes on the TaskManager
 * instance so the private methods are all unsynchronized. There are a few getter methods with default visibility. The
 * caller of these should ensure that he synchronizes on the TaskManager.
 */
public class TaskManager extends Observable {
	/**
	 * A Timer Task used to keep an eye on the TaskInvocator threads
	 */
	private class TaskInvocatorMonitor extends TimerTask {
		/**
		 * A time period in millis , used to give the TaskInvocator a chance to clean up a task before clobbering it
		 */
		private long m_interrptDelayMillis;

		/**
		 * Creates a new TaskInvocatorMonitor object.
		 * 
		 * @param interrptDelayMillis
		 *            A time period in millis, used to give the TaskInvocator a chance to clean up a task before
		 *            clobbering it
		 */
		TaskInvocatorMonitor(long interrptDelayMillis) {
			m_interrptDelayMillis = interrptDelayMillis;

			if (LOG.isInfoEnabled()) {
				LOG.info("Created TaskInvocatorMonitor timer");
			}
		}

		/**
		 * Periodically check TaskInvocatorDataList for over running tasks, over running is considered to be task
		 * EndTime + a bit, to give the TaskInvocator a chance to clean up
		 */
		@Override
		public void run() {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Starting check for hung task invocators");
			}

			synchronized (TaskManager.this) {
				// Get a list of invocators currently running tasks
				Set<Thread> invocatorKeys = m_taskInvocatorDataList.keySet();
				LinkedList<Thread> badInvocators = new LinkedList<Thread>();

				// Check each invocator running a task
				for (Thread invocator : invocatorKeys) {
					TaskInvocatorData taskInvocatorData = m_taskInvocatorDataList.get(invocator);

					// Check if task has out stayed its welcome
					if (System.currentTimeMillis() > (taskInvocatorData.getTaskEndTimeMillis() + m_interrptDelayMillis)) {
						badInvocators.add(invocator);
					}
				}

				// process hung taskInvocators
				for (Thread invocator : badInvocators) {
					TaskInvocatorData taskInvocatorData = m_taskInvocatorDataList.get(invocator);
					boolean goodOnlyInvocator = taskInvocatorData.isGoodOnlyInvocator();
					Task task = taskInvocatorData.getTask();
					String key = task.getKey();

					// Remove record from taskInvocator map
					m_taskInvocatorDataList.remove(invocator);

					// Attempt to kill old TaskInnvocator
					invocator.interrupt();

					LOG.warn("Task: " + task + " out stayed its welcome, sent interupt to invocator: " + invocator);

					// Set task.setResultCode = HARD_ERROR
					task.setResultCode(Result.HARD_ERROR);
					incrementFailedTasks();
					updateLastTaskTime();

					// Remove task.key from goodKey set
					getGoodKeys().remove(key);

					// removeSimilarTasks
					for (Iterator<Task> iter = m_tasks.iterator(); iter.hasNext();) {
						Task otask = iter.next();
						if (otask.getKey().equals(key)) {
							iter.remove();
							otask.setResultCode(Task.Result.HARD_ERROR);
							incrementFailedTasks();
						}
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("Removed all tasks with key \" " + key + "\" from the task queue.");
					}

					// Remove task.key from currentTasks map
					currentTasksRemoveKey(key);
					m_firstRejectedTaskAnyInvocator = null;
					m_firstRejectedTaskGoodOnlyInvocator = null;

					TaskManager.this.notifyAll();

					// find stalled invocator thread in thread pool
					int i;

					for (i = 0; i < s_taskInvocators.length; i++) {
						if (s_taskInvocators[i] == invocator) {
							break;
						}
					}

					// Create new TaskInnvocator with same properties as old
					// thread i.e. same goodOnlyInvocator value, to replace the
					// stalled thread in the thread pool

					TaskInvocator inv = new TaskInvocator(TaskManager.this, goodOnlyInvocator);
					s_taskInvocators[i] = inv;
					inv.setDaemon(true);
					inv.start();
					m_lastTaskInvocatorThreadNum++;
					inv.setName("taskInvocator-" + String.valueOf(m_lastTaskInvocatorThreadNum));
					m_lastTaskInvocatorThreadNum = i;
					if (LOG.isInfoEnabled()) {
						LOG.info("Created replacement TaskInvocator - TaskInvocator-" + String.valueOf(m_lastTaskInvocatorThreadNum));
					}
				}
			}

			if (LOG.isInfoEnabled()) {
				LOG.info("Finished check for hung task invocators");
			}
		}
	}

	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger(TaskConstants.TASK_LOGGER);

	/** No. of failed tasks since start-up */
	private int m_failedTasks;

	/** To synchronize get and drop instance calls */
	private static Object s_instanceLock = new Object();

	/** Time last task was processed */
	private long m_lastTaskTimeMillis;

	/** No. of successful tasks since start-up */
	private int m_successfulTasks;

	/** Internal reference to this singleton */
	private static TaskManager s_taskManager;

	/** A timer to monitor the taskInvocation threads and interrupt them if they hang */
	private Timer s_taskInvocatorMonitorTimer;

	/** Pool of threads used to make calls. */
	private TaskInvocator[] s_taskInvocators;

	/** The maximum number of tasks that are queued and would run if there were a slot. */
	private int m_maximumGoodQueuedTaskCount;

	/** Map from Key object to number of threads currently attempting to invoke a task on the key */
	private Map<String, Integer> m_currentTasks;

	/** The first rejected task by an 'Any' invocator while looping through the queue. */
	private Task m_firstRejectedTaskAnyInvocator;

	/** The first rejected task by a 'Good Only' invocator while looping through the queue. */
	private Task m_firstRejectedTaskGoodOnlyInvocator;

	/** Good set of keys for which invocations can be made concurrently. */
	private Set<String> m_goodKeys;

	/** Holds the number of threads configured to process tasks with good key only */
	private int m_goodOnlyThreads;

	/** The numeric part of the last TaskInvocator thread name */
	private int m_lastTaskInvocatorThreadNum;

	/** Tasks to invoke. */
	private Queue<Task> m_tasks;

	/** Data about a TaskInvocator and the current task that it is running. */
	private final Map<Thread, TaskInvocatorData> m_taskInvocatorDataList = new HashMap<Thread, TaskInvocatorData>();

	/** Number of threads in pool. */
	private int m_threadsInPool;

	/**
	 * FOR TESTING ONLY !!! Enables tests to recreate instance with different parameters.
	 */
	public static void dropInstance() {
		synchronized (s_instanceLock) {
			if (s_taskManager != null) {
				s_taskManager.shutdown();
				s_taskManager = null;
			}
		}
	}

	public static TaskManager getInstance() throws RGMAPermanentException {
		synchronized (s_instanceLock) {
			if (s_taskManager == null) {
				s_taskManager = new TaskManager();
			}
			return s_taskManager;
		}
	}

	/**
	 * Constructs a new TaskManager object.
	 * 
	 * @param threadsInPool
	 *            Number of threads in the thread pool.
	 * @param m_goodOnlyThreads
	 *            Number of threads in the thread pool that should only handle tasks that have their keys in the
	 *            goodKeys set.
	 * @throws ConfigurationException
	 *             Indicates error in configuration parameters.
	 */
	private TaskManager() throws RGMAPermanentException {
		LOG.debug("Creating TaskManager");

		ServerConfig config = ServerConfig.getInstance();

		m_threadsInPool = config.getInt(ServerConstants.TASKMANAGER_THREADS_IN_POOL);
		m_goodOnlyThreads = config.getInt(ServerConstants.TASKMANAGER_GOOD_ONLY_THREADS);
		m_maximumGoodQueuedTaskCount = config.getInt(ServerConstants.TASKMANAGER_MAXIMUM_GOOD_QUEUED_TASK_COUNT);

		if (m_threadsInPool < 1) {
			throw new RGMAPermanentException("Illegal cofiguartion parameter specified for number of threads in TaskManager thread pool. Must be > 0");
		}

		/**
		 * Ensures at least one thread is not a goodOnlyThread and goodOnlyThread is not greater than threadsInpool
		 */
		if (m_goodOnlyThreads >= m_threadsInPool) {
			throw new RGMAPermanentException(
					"Illegal cofiguartion parameter specified for number of threads in TaskManager thread pool. Must be > number of 'good only' threads");
		}

		m_tasks = new LinkedList<Task>();
		m_currentTasks = new HashMap<String, Integer>();
		m_goodKeys = new HashSet<String>();
		s_taskInvocators = new TaskInvocator[m_threadsInPool];

		// Start the invocator threads
		for (int i = 0; i < s_taskInvocators.length; i++) {
			TaskInvocator inv;
			if (i < m_goodOnlyThreads) {
				inv = new TaskInvocator(this, true);
			} else {
				inv = new TaskInvocator(this, false);
			}
			s_taskInvocators[i] = inv;
			inv.setDaemon(true);
			inv.start();
			m_lastTaskInvocatorThreadNum++;
			inv.setName("TaskInvocator-" + m_lastTaskInvocatorThreadNum);
		}

		// Create a timer task to check for stalled TaskInvocators
		long threadInterruptPeriodMillis = config.getLong(ServerConstants.TASKMANAGER_HANGING_INVOCATORS_CHECK_PERIOD_SECS) * 1000;
		long threadInterruptDelayMillis = config.getLong(ServerConstants.TASKMANAGER_HANGING_INVOCATORS_CHECK_DELAY_SECS) * 1000;
		TimerTask threadInterrupt = new TaskInvocatorMonitor(threadInterruptDelayMillis);
		s_taskInvocatorMonitorTimer = new Timer("TaskInvocatorMonitor", true);
		s_taskInvocatorMonitorTimer.schedule(threadInterrupt, threadInterruptPeriodMillis, threadInterruptPeriodMillis);

		LOG.info("Created TaskInvocatorMonitor timer with a repeat period of " + threadInterruptPeriodMillis / 1000 + " seconds");

		// Ensure start time info are initialized to a sensible value
		updateLastTaskTime();
		m_successfulTasks = 0;
		m_failedTasks = 0;

		LOG.info("Created thread pool of " + m_threadsInPool + " threads, of which " + m_goodOnlyThreads + " are goodOnly threads");
	}

	/**
	 * Adds a Task to the list of tasks to execute.
	 * 
	 * @param Task
	 *            Task to execute.
	 */
	public synchronized void add(Task task) {
		m_firstRejectedTaskAnyInvocator = null;
		m_firstRejectedTaskGoodOnlyInvocator = null;

		m_tasks.add(task);
		this.notifyAll();

		if (LOG.isInfoEnabled()) {
			LOG.info("Added: " + task + " to the task queue");
		}
	}

	public synchronized void checkBusy() throws RGMATemporaryException {
		if (m_tasks.size() > m_maximumGoodQueuedTaskCount) {
			int n = 0;
			for (Task task : m_tasks) {
				String key = task.getKey();
				if (m_goodKeys.contains(key)) {
					if (++n > m_maximumGoodQueuedTaskCount) {
						throw new RGMATemporaryException("Server busy - too many queued tasks");
					}
				}
			}
		}
	}

	/**
	 * Returns a copy of the list of all the good keys.
	 * 
	 * @return A list of good keys
	 */
	public synchronized List<String> getCopyGoodKeys() {
		return new ArrayList<String>(m_goodKeys);
	}

	/**
	 * Return a list of all task keys not registered as good keys for queued and running tasks.
	 * 
	 * @return A list of not good keys
	 */
	public synchronized List<String> getNotGoodKeys() {
		Map<String, String> notGoodKeys = new HashMap<String, String>();
		for (Task each : m_tasks) {
			String extracted = each.getKey();
			if (!m_goodKeys.contains(extracted)) {
				notGoodKeys.put(extracted, "");
			}
		}

		Set<String> keys = m_currentTasks.keySet();
		for (String each : keys) {
			if (!m_goodKeys.contains(each)) {
				notGoodKeys.put(each, "");
			}
		}
		return new ArrayList<String>(notGoodKeys.keySet());
	}

	/**
	 * Retrieves a copy of the task queue.
	 * 
	 * @return A list of tasks
	 */
	public synchronized List<Task> getQueuedTasks() {
		return new ArrayList<Task>(m_tasks);
	}

	/**
	 * Retrieves a copy of the list of tasks currently being processed by TaskInvocators.
	 * 
	 * @return A list of tasks
	 */
	public synchronized List<Task> getRunningTasks() {
		List<Task> tasks = new ArrayList<Task>();
		// Check each invocator running a task
		for (TaskInvocatorData tid : m_taskInvocatorDataList.values()) {
			Task task = tid.getTask();
			if (task != null) {
				tasks.add(task);
			}
		}
		return tasks;
	}

	/**
	 * Retrieves all the queued tasks that are added by the specified owner. NB this does not include running tasks.
	 * 
	 * @param owner
	 *            A string representing an identifier for the task owner
	 * @return A list of string array containing the key and the status (result) of the queued tasks for the specified
	 *         owner.
	 */
	public synchronized List<Map<String, String>> getTasks(String owner) {
		List<Map<String, String>> allOwnerTasks = new ArrayList<Map<String, String>>();
		for (Task task : m_tasks) {
			if (task.getOwnerId().equalsIgnoreCase(owner)) {
				Map<String, String> status = task.statusInfo();
				allOwnerTasks.add(status);
			}
		}
		return allOwnerTasks;
	}

	/**
	 * Returns the number of tasks per key for queued and running tasks.
	 * 
	 * @return A map containing the number of tasks keyed on the key for each task.
	 */
	public synchronized Map<String, Integer> getTasksPerKey() {
		// First get the current ones
		Map<String, Integer> map = new HashMap<String, Integer>(m_currentTasks);

		// Then add those in the queue
		for (Task task : m_tasks) {
			String key = task.getKey();
			if (map.containsKey(key)) {
				int value = map.get(key);
				map.put(key, ++value);
			} else {
				map.put(key, 1);
			}
		}

		return map;
	}

	/**
	 * Retrieves the status info for the task invocators.
	 * 
	 * @return A map of status information for monitoring purposes
	 */
	public synchronized Map<String, String> invocatorStatusInfo() {
		Map<String, String> map = new HashMap<String, String>();
		for (TaskInvocator inv : s_taskInvocators) {
			map.put(String.valueOf(inv.getId()), String.valueOf(inv.goodOnly()));
		}
		return map;
	}

	/**
	 * Retrieves the status info for this task manager.
	 * 
	 * @return A map of status information for monitoring purposes, containing: NoOfTaskInQueue,
	 *         NoOfGoodOnlyTaskThreads, NoOfAnyTaskThreads, LastTaskCompletedTimeMillis, LastTaskCompletedTimeString,
	 *         SuccessfulTasks, FailedTasks, AllTasks
	 */
	public synchronized Map<String, String> statusInfo() {
		Map<String, String> map = new HashMap<String, String>();

		map.put("NoOfTaskInQueue", String.valueOf(m_tasks.size()));
		map.put("NoOfGoodOnlyTaskThreads", String.valueOf(m_goodOnlyThreads));
		map.put("NoOfAnyTaskThreads", String.valueOf(m_threadsInPool - m_goodOnlyThreads));
		map.put("LastTaskCompletedIntervalMillis", String.valueOf(System.currentTimeMillis() - m_lastTaskTimeMillis));
		map.put("CountSuccessfulTasks", String.valueOf(m_successfulTasks));
		map.put("CountFailedTasks", String.valueOf(m_failedTasks));
		map.put("CountAllExecutedTasks", String.valueOf(m_successfulTasks + m_failedTasks));
		return map;
	}

	synchronized void addTaskInvocatorDataList(Thread id, TaskInvocatorData taskInvocatorData) {
		m_taskInvocatorDataList.put(id, taskInvocatorData);
	}

	synchronized void currentTasksRemoveKey(String key) {
		int count = 0;
		if (m_currentTasks.containsKey(key)) {
			count = m_currentTasks.get(key);
			m_currentTasks.put(key, --count);

			if (count == 0) {
				m_currentTasks.remove(key);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Removed key " + key + " from current task list.");
				}
			}
		}
	}

	Task getFirstRejectedTaskAnyInvocator() {
		return m_firstRejectedTaskAnyInvocator;
	}

	Task getFirstRejectedTaskGoodOnlyInvocator() {
		return m_firstRejectedTaskGoodOnlyInvocator;
	}

	Map<String, Integer> getCurrentTasks() {
		return m_currentTasks;
	}

	Set<String> getGoodKeys() {
		return m_goodKeys;
	}

	Queue<Task> getTasks() {
		return m_tasks;
	}

	/**
	 * This method is needed to be able to test that hanging threads get replaced
	 * 
	 * @return a list of TaskInvocator threads
	 */
	Thread[] getThreadPool() {
		return s_taskInvocators;
	}

	/**
	 * Increment the count of failed tasks
	 */
	synchronized void incrementFailedTasks() {
		m_failedTasks++;
	}

	/**
	 * Increment the count of successful tasks
	 */
	synchronized void incrementSuccessfulTasks() {
		m_successfulTasks++;
	}

	/**
	 * Reset m_firstRejectedTaskAny and m_firstRejectedTaskGoodOnly to null
	 */
	synchronized void resetFirstRejectedTasks() {
		m_firstRejectedTaskAnyInvocator = null;
		m_firstRejectedTaskGoodOnlyInvocator = null;
		this.notifyAll();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Reset first task for 'Any' and 'Good Only' invocators to null");
		}
	}

	void setFirstRejectedTaskAnyInvocator(Task task) {
		m_firstRejectedTaskAnyInvocator = task;
	}

	void setFirstRejectedTaskGoodOnlyInvocator(Task task) {
		m_firstRejectedTaskGoodOnlyInvocator = task;
	}

	void shutdown() {
		s_taskInvocatorMonitorTimer.cancel();
		for (TaskInvocator ti : s_taskInvocators) {
			ti.shutdown();
		}
	}

	synchronized boolean taskInvocatorDataListHasKey(Thread id) {
		return m_taskInvocatorDataList.containsKey(id);
	}

	synchronized void taskInvocatorDataListRemoveKey(Thread id) {
		m_taskInvocatorDataList.remove(id);
	}

	/**
	 * Update the time that a task last completed
	 */
	synchronized void updateLastTaskTime() {
		m_lastTaskTimeMillis = System.currentTimeMillis();
	}
}
