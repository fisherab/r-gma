/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.tasks;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.tasks.Task.Result;

/**
 * This class serves to periodically pop tasks from the queue and invoke them.
 */
public class TaskInvocator extends Thread {
	/** Reference to logging utility. */
	private static final Logger LOG = Logger.getLogger(TaskConstants.TASK_LOGGER);

	/** The task currently being processed. */
	private Task m_currentTask;

	/** Whether to only process tasks with good keys. */
	private boolean m_goodOnlyInvocator;

	/** Flag to mark task as having timed out. */
	private boolean m_taskTimedOut;

	/** Name of the current thread. */
	String m_threadName;

	/** ID of the current thread. */
	private Thread m_threadId;

	/** The parent TaskManager */
	private TaskManager m_taskManager;

	Timer m_timer;

	/**
	 * Creates a new TaskInvocator object.
	 * 
	 * @param currentTasks
	 *            A reference to a collection of tasks that are currently being processed.
	 * @param firstRejectedTaskAnyInvocator
	 *            A reference to the first task to be rejected by an 'Any' invocator.
	 * @param firstRejectedTaskGoodOnlyInvocator
	 *            A reference to the first task to be rejected by a 'Good Only' invocator.
	 * @param goodKeys
	 *            A reference to the good keys set.
	 * @param goodOnlyInvocator
	 *            A flag indicating whether this invocator should only handle tasks that have their keys in the good
	 *            keys set.
	 * @param taskInvocatorDataList
	 *            Information about currently invoked tasks and the invoking invocators.
	 * @param tasks
	 *            A reference to the list of tasks to invoke.
	 * @param taskLastOrdersPeriodMillis
	 *            The period to allow for last orders on a task before timing it out.
	 */
	public TaskInvocator(TaskManager taskManager, boolean goodOnlyInvocator) {
		super();
		m_taskManager = taskManager;
		m_goodOnlyInvocator = goodOnlyInvocator;
		m_timer = new Timer("TaskMonitor", true);
	}

	void shutdown() {
		LOG.debug("Shutting down " + this);
		interrupt();
	}

	/**
	 * The type of tasks processed by this invocator
	 * 
	 * @return true if this invocator only processes tasks with good keys
	 */
	boolean goodOnly() {
		return m_goodOnlyInvocator;
	}

	/**
	 * Main loop of the thread. Waits for a new task to be added to the queue, then attempts to process it.
	 */
	public void run() {
		LOG.debug("Entering run()");

		m_threadId = Thread.currentThread();
		m_threadName = getName();

		try {
			while (!m_threadId.isInterrupted()) {
				synchronized (m_taskManager) {
					Queue<Task> tasks = m_taskManager.getTasks();
					while ((tasks.size() == 0) | (m_goodOnlyInvocator & (tasks.peek() == m_taskManager.getFirstRejectedTaskGoodOnlyInvocator()))
							| (!m_goodOnlyInvocator & (tasks.peek() == m_taskManager.getFirstRejectedTaskAnyInvocator()))) {
						if (LOG.isDebugEnabled()) {
							if (tasks.size() == 0) {
								LOG.debug((m_goodOnlyInvocator ? "Good only" : "Any") + " invocator - no queued tasks - wait for addition to queue...");
							} else {
								LOG.debug((m_goodOnlyInvocator ? "Good only" : "Any") + " invocator - wrapped round queue- wait for addition to queue...");
							}
						}
						m_taskManager.wait();
					}

					m_currentTask = tasks.remove();

					if (LOG.isDebugEnabled()) {
						LOG.debug((m_goodOnlyInvocator ? "Good only" : "Any") + " invocator - got next task - queue size now " + tasks.size());
					}
				}

				// If the task has not been aborted
				if (m_currentTask.getResultCode() != Result.HARD_ERROR) {
					String currentKey = m_currentTask.getKey();
					boolean processTask = false;

					synchronized (m_taskManager) {
						Set<String> goodKeys = m_taskManager.getGoodKeys();
						Map<String, Integer> currentTasks = m_taskManager.getCurrentTasks();
						boolean keyInUse = currentTasks.containsKey(currentKey) && currentTasks.get(currentKey) != 0;
						if ((goodKeys.contains(currentKey)) | (!m_goodOnlyInvocator & !keyInUse)) {
							// Add key entry to m_currentTasks
							if (currentTasks.containsKey(currentKey)) {
								int count = currentTasks.get(currentKey);
								currentTasks.put(currentKey, ++count);
							} else {
								currentTasks.put(currentKey, 1);
							}
							processTask = true;
						}
					}

					if (processTask) {
						m_taskManager.resetFirstRejectedTasks();

						Result result = null;
						m_currentTask.incrementAttempts();

						m_taskTimedOut = false;
						ThreadInterrupt ti = new ThreadInterrupt(m_threadId, m_currentTask);

						long timeoutMillis = m_currentTask.getMaxRunTimeMillis();
						long endTimeMillis = timeoutMillis + System.currentTimeMillis();
						TaskInvocatorData taskInvocatorData = new TaskInvocatorData(this, m_goodOnlyInvocator, m_currentTask, endTimeMillis);
						m_taskManager.addTaskInvocatorDataList(m_threadId, taskInvocatorData);

						// Set the timer and call method on the task
						m_timer.schedule(ti, timeoutMillis);

						setName("Task:" + m_currentTask.getOwnerId());
						if (LOG.isDebugEnabled()) {
							LOG.debug("Invoking task " + m_currentTask + "with timeout of " + timeoutMillis);
						}
						try {
							result = m_currentTask.invoke();
						} catch (Throwable e) {
							setName(m_threadName);
							LOG.warn("Caught exception when calling task " + m_currentTask, e);
							result = Result.HARD_ERROR;
						} finally {
							setName(m_threadName);
						}

						// Stop the timer task as soon as possible
						ti.cancel();

						// If record already removed from taskInvocatorDataList - commit suicide
						// This is because the thread has been abandoned by the task manager
						// and a replacement created
						if (!m_taskManager.taskInvocatorDataListHasKey(m_threadId)) {
							LOG.warn("Task " + m_currentTask + " in TaskInvocator abandonded by TaskManager, stopping invocator");
							throw new InterruptedException();
						}

						// Remove record from taskInvocatorDataList as now the task has run
						m_taskManager.taskInvocatorDataListRemoveKey(m_threadId);

						if (LOG.isDebugEnabled()) {
							LOG.debug("Task " + m_currentTask + " removed from taskInvocatorDataList");
						}

						if (LOG.isInfoEnabled()) {
							LOG.info("Task " + m_currentTask + " finished");
						}

						m_taskManager.updateLastTaskTime();
						processTaskResult(result);
					} else {
						// Put it back on to the queue.
						synchronized (m_taskManager) {
							Queue<Task> tasks = m_taskManager.getTasks();
							if (m_goodOnlyInvocator) {
								if (m_taskManager.getFirstRejectedTaskGoodOnlyInvocator() == null) {
									m_taskManager.setFirstRejectedTaskGoodOnlyInvocator(m_currentTask);
									if (LOG.isDebugEnabled()) {
										LOG.debug("Reset first task for 'Good Only' invocator to " + m_taskManager.getFirstRejectedTaskGoodOnlyInvocator());
									}
								}
							} else {
								if (m_taskManager.getFirstRejectedTaskAnyInvocator() == null) {
									m_taskManager.setFirstRejectedTaskAnyInvocator(m_currentTask);
									if (LOG.isDebugEnabled()) {
										LOG.debug("Reset first task for 'Any' invocator to " + m_taskManager.getFirstRejectedTaskAnyInvocator());
									}
								}
							}
							tasks.add(m_currentTask);
							m_taskManager.notifyAll();
						}
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug((m_goodOnlyInvocator ? "Good only" : "Any") + " invocator not processing task " + m_currentTask + "now back on queue");
					}
				}
			}
		} catch (InterruptedException e) {
			LOG.info("TaskInvocator Thread was interrupted - exiting.");
		} catch (Throwable t) {
			LOG.error("Throwable caught in TaskInvocator thread", t);
		}
		m_timer.cancel();
	}

	/**
	 * Tries to invoke the task.
	 * 
	 * @param goodKeys
	 * @param tasks
	 */
	private void processTaskResult(Result result) throws InterruptedException {

		if ((result != Result.SUCCESS) & (result != Result.SOFT_ERROR) & (result != Result.HARD_ERROR)) {
			result = Result.HARD_ERROR;
			LOG.error("Unknown result returned from task " + m_currentTask + " - set to HARD_ERROR");
		}

		if (m_taskTimedOut) {
			// clear thread interrupt flag, just in case it is still set
			Thread.interrupted();
			result = Result.HARD_ERROR;
			LOG.warn("Task " + m_currentTask + " timed out");
		}

		if ((result == Result.SOFT_ERROR) & (m_currentTask.getCurrentAttemptNumber() >= m_currentTask.getMaxAttemptCount())) {
			result = Result.HARD_ERROR;
			if (LOG.isDebugEnabled()) {
				LOG.warn("Task " + m_currentTask + " exceeded maximum attempt count");
			}
		}

		if (m_currentTask.getResultCode() == Result.HARD_ERROR) {
			// Abort flag has been set
			m_taskManager.currentTasksRemoveKey(m_currentTask.getKey());
			m_taskManager.resetFirstRejectedTasks();
			if (LOG.isInfoEnabled()) {
				LOG.warn("For task " + m_currentTask + " the abort flag was set while the task was running");
			}
			return;
		}

		m_currentTask.setResultCode(result);

		String key = m_currentTask.getKey();
		if (result == Result.SUCCESS) {
			synchronized (m_taskManager) {
				Set<String> goodKeys = m_taskManager.getGoodKeys();
				if (m_currentTask.getCurrentAttemptNumber() == 1) {
					goodKeys.add(key);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Task " + m_currentTask + " was successful on first attempt - added " + key + " to goodKey set.");
					}
				} else {
					goodKeys.remove(key);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Task " + m_currentTask + " only successful on retry - removed " + key + " from goodKey set.");
					}
				}
			}
			// Remove key from to m_currentTasks as early as possible
			m_taskManager.currentTasksRemoveKey(key);
			m_taskManager.resetFirstRejectedTasks();
			m_taskManager.incrementSuccessfulTasks();

		} else if (result == Result.SOFT_ERROR) {
			// There was a problem with the task so remove the key from to m_goodKeys set
			synchronized (m_taskManager) {
				Set<String> goodKeys = m_taskManager.getGoodKeys();
				Queue<Task> tasks = m_taskManager.getTasks();
				goodKeys.remove(key);
				m_taskManager.currentTasksRemoveKey(key);
				m_taskManager.resetFirstRejectedTasks();
				if (m_currentTask.getResultCode() == Result.HARD_ERROR) {
					if (LOG.isInfoEnabled()) {
						LOG.warn("For task " + m_currentTask + " the abort flag was set while the task result was being processed");
					}
				} else {
					tasks.add(m_currentTask);
					m_taskManager.notifyAll();
				}
				if (LOG.isInfoEnabled()) {
					LOG.info("Task " + m_currentTask + " gave SOFT_ERROR - now back on queue");
				}
			}

		} else { // if (result == Result.HARD_ERROR)
			synchronized (m_taskManager) {
				Set<String> goodKeys = m_taskManager.getGoodKeys();
				Queue<Task> tasks = m_taskManager.getTasks();
				goodKeys.remove(key);
				for (Iterator<Task> iter = tasks.iterator(); iter.hasNext();) {
					Task otask = iter.next();
					if (otask.getKey().equals(key)) {
						iter.remove();
						otask.setResultCode(Task.Result.HARD_ERROR);
						m_taskManager.incrementFailedTasks();
					}
				}
			}

			// Remove key from to m_currentTasks as early as possible
			m_taskManager.currentTasksRemoveKey(key);
			m_taskManager.resetFirstRejectedTasks();

			if (LOG.isInfoEnabled()) {
				LOG.info("Task " + m_currentTask + " gave HARD_ERROR - all tasks with key \" " + key + "\" removed from the queue.");
			}
			m_taskManager.incrementFailedTasks();
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Task " + m_currentTask + " returned code " + m_currentTask.getResultCode());
		}
	}

	/**
	 * Create a timer task to monitor the invoked task.
	 * 
	 * @return A timer task
	 */
	private class ThreadInterrupt extends TimerTask {

		private Thread thread;
		private Task task;

		ThreadInterrupt(Thread thread, Task task) {
			this.thread = thread;
			this.task = task;
		}

		public void run() {
			this.thread.interrupt();
			m_taskTimedOut = true;

			if (LOG.isInfoEnabled()) {
				LOG.info("TimerTask " + this.task + " timed out, sent interrupt");
			}
		}
	}
}
