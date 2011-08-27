/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.tasks;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates a remote invocation.
 */
public abstract class Task {
	/** Number of attempts made to invoke this task. */
	private int m_attempts;

	/** "Key" (usually host name) that identifies the resource to be used. */
	private String m_key;

	/** Time period in milliseconds after which the task will be interrupted. */
	private long m_maxRunTimeMillis;

	/** Result of the last invocation. */
	private Result m_resultCode;

	/** Task owners identity */
	private String m_ownerId;

	/** Maximum number of soft errors */
	private int m_maxAttemptCount;

	/** The time that the task was created * */
	private long m_creationTimeMillis;

	/**
	 * Abstract constructor.
	 * 
	 * @param ownerId
	 *            A string representing the identity of the owner of this task.
	 * @param key
	 *            A string representation of the "scarce resource" that this
	 *            task is dependent upon.
	 * @param maxRunTimeMillis
	 *            The time in milliseconds the task should be allowed to run
	 *            before being interrupted by the task manager.
	 * @param maxAttemptCount
	 *            If a soft error is returned and this count has been reached
	 *            the soft error will be made hard
	 */
	public Task(String ownerId, String key, long maxRunTimeMillis, int maxAttemptCount) {
		m_key = key;
		m_ownerId = ownerId;
		m_maxRunTimeMillis = maxRunTimeMillis;
		m_maxAttemptCount = maxAttemptCount;
		m_attempts = 0;
		m_creationTimeMillis = System.currentTimeMillis();
	}

	/**
	 * Invokes this task and returns a status code.
	 * 
	 * @return The status of the task.
	 * 
	 * @see Result
	 */
	public abstract Result invoke();

	/**
	 * Aborts this invocation, stopping it from being retried.
	 */
	public void abort() {
		if ((m_resultCode == null) || (m_resultCode == Result.SOFT_ERROR)) {
			setResultCode(Result.HARD_ERROR);
		}
	}

	/**
	 * Gets the result code for the last attempted invocation, this will be NULL
	 * if the task has not yet been invoked.
	 * 
	 * @return Returns the result code.
	 */
	public Result getResultCode() {
		return m_resultCode;
	}

	/**
	 * Return the ownerId and the key
	 * 
	 * @return Returns a string containing ownerId and the key
	 */
	public String toString() {
		return "OwnerId='" + m_ownerId + "' Key='" + m_key + "'";
	}

	/**
	 * Gets the number of attempts that have been made to execute this task.
	 * 
	 * @return An integer representing number of attempts so far.
	 */
	protected int getCurrentAttemptNumber() {
		return m_attempts;
	}

	int getMaxAttemptCount() {
		return m_maxAttemptCount;
	}

	/**
	 * Increments the number of attempts that have been made to execute this
	 * task by 1.
	 */
	void incrementAttempts() {
		m_attempts++;
	}

	/**
	 * Gets the key for this task.
	 * 
	 * @return Returns the key.
	 */
	String getKey() {
		return m_key;
	}

	/**
	 * Gets the maximum time in milliseconds that the task is expected to run.
	 * NB after this time the task can expect to be interrupted.
	 * 
	 * @return Returns time period in milliseconds
	 */
	long getMaxRunTimeMillis() {
		return m_maxRunTimeMillis;
	}

	/**
	 * Gets the owner ID for this task.
	 * 
	 * @return Returns the owner ID.
	 */
	String getOwnerId() {
		return m_ownerId;
	}

	/**
	 * Sets the result code if it is not already Result.SUCCESS or
	 * Result.HARD_ERROR.
	 * 
	 * @param resultCode
	 *            The value to set the result code to.
	 */
	synchronized void setResultCode(Result resultCode) {
		if ((m_resultCode != Result.SUCCESS) && (m_resultCode != Result.HARD_ERROR)) {
			m_resultCode = resultCode;
		}
	}

	/**
	 * Retrieves the status info for this task .
	 * 
	 * @return A map of status information for monitoring purposes.
	 */
	public Map<String, String> statusInfo() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("Key", String.valueOf(m_key));
		map.put("Owner", String.valueOf(m_ownerId));
		map.put("ResultCode", String.valueOf(m_resultCode));
		map.put("Attempts", String.valueOf(m_attempts));
		map.put("MaxAttemptCount", String.valueOf(m_maxAttemptCount));
		map.put("MaxRunTimeMillis", String.valueOf(m_maxRunTimeMillis));
		map.put("CreationTimeMillis", String.valueOf(m_creationTimeMillis));
		return map;
	}

	/** Result of the invocation. */
	public enum Result {
		SUCCESS, SOFT_ERROR, HARD_ERROR;
	}
}
