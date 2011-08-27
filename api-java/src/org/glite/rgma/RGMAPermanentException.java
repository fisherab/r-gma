package org.glite.rgma;

/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/**
 * Exception thrown when it is unlikely that repeating the call will be successful. Normally the application writer
 * should investigate the cause of the problem.
 */
@SuppressWarnings("serial")
public class RGMAPermanentException extends RGMAException {

	/**
	 * Constructs an RGMAPermanentException object.
	 * 
	 * @param message
	 *            Error message.
	 */
	RGMAPermanentException(String message) {
		super(message);
	}
}
