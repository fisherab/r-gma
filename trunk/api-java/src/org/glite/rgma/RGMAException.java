/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma;

/**
 * Exception thrown when an error occurs in the RGMA application.
 */
@SuppressWarnings("serial")
public class RGMAException extends Exception {

	/** Number of successful batch operations. */
	private int m_numSuccessfulOps;

	/**
	 * Creates a new RGMAException object.
	 * 
	 * @param message
	 *            Error message.
	 */
	protected RGMAException(String message) {
		super(message);
	}

	/**
	 * Returns the number of successful operations for the call. For the majority of calls, that are only expected to
	 * carry out one operation, this value will be zero.
	 * 
	 * @return the number of successful operations.
	 */
	public int getNumSuccessfulOps() {
		return m_numSuccessfulOps;
	}

	/**
	 * Sets the number of successful operations (for batch commands).
	 * 
	 * @param numSuccessfulOps
	 *            Number of successful operations (for batch commands).
	 */
	protected void setNumSuccessfulOps(int numSuccessfulOps) {
		m_numSuccessfulOps = numSuccessfulOps;
	}
}
