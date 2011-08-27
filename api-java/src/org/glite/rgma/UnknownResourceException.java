/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

/**
 * Internal Exception thrown when a resource cannot be found in the resource framework.
 */
@SuppressWarnings("serial")
class UnknownResourceException extends Exception {
	
	UnknownResourceException() {
		super("Unknown resource.");
	}
	
}
