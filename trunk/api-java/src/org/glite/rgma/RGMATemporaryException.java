/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

/**
 * Exception thrown when an R-GMA call fails, but calling the method again after a delay may be
 * successful.
 */
@SuppressWarnings("serial")
public class RGMATemporaryException extends RGMAException {

	RGMATemporaryException(String message) {
		super(message);
	}
}
