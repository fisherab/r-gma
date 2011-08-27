/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.consumer;

import org.glite.rgma.server.system.RGMAException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSet;

/**
 * Interface to any object which can behave as a Consumer, i.e. Consumer or Secondary Producer.
 */
public interface Consumable {
	/**
	 * Consume results.
	 * @param vdbTableName 
	 */
	public void push(TupleSet rs, String vdbTableName);

	/**
	 * Get the endpoint of the consumer-type object
	 */
	public ResourceEndpoint getEndpoint();

	/**
	 * Remove a producer from the consumer's plan
	 */
	public void removeProducer(ResourceEndpoint producer);

	/**
	 * Stop the query and record the exception to be thrown on subsequent pop before self destruction
	 * 
	 * @throws RGMAException
	 */
	public void abend(RGMAException e);
}
