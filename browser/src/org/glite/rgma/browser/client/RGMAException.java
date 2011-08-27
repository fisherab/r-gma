/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.browser.client;

@SuppressWarnings("serial")
public class RGMAException extends Exception {

	private int m_numSuccessfulOps;

	protected RGMAException(String message) {
		super(message);
	}

	public int getNumSuccessfulOps() {
		return m_numSuccessfulOps;
	}

	protected void setNumSuccessfulOps(int numSuccessfulOps) {
		m_numSuccessfulOps = numSuccessfulOps;
	}
}
