/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

@SuppressWarnings("serial")
public class RGMAPermanentException extends RGMAException {

	public RGMAPermanentException(String message) {
		super(message);
	}

	public RGMAPermanentException(String message, Throwable cause) {
		super(message, cause);
	}

	public RGMAPermanentException(Throwable cause) {
		super(cause);
	}

}
