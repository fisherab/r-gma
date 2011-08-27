/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * Exception thrown when a resource cannot be found in  the resource framework.
 */
@SuppressWarnings("serial")
public class UnknownResourceException extends Exception {
 
    /**
     * Constructs an UnknownResourceException object.
     *
     * @param endpoint Endpoint of the resource which could not be found.
     */
    public UnknownResourceException(ResourceEndpoint endpoint) {
        super("Resource has been closed: " + endpoint);
    }

    public UnknownResourceException(String message) {
        super(message);
    }
}