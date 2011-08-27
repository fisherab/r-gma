/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides various static methods.
 */
public class RGMAService {

	private RGMAService() {}

	/**
	 * Returns a list of existing tuple stores.
	 * 
	 * @return a list of tuple store objects that belong to you
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public static List<TupleStore> listTupleStores() throws RGMAPermanentException, RGMATemporaryException {
		List<TupleStore> stores = new ArrayList<TupleStore>();
		ServletConnection connection = new ServletConnection("RGMAService");
		for (Tuple t : connection.sendNonResourceCommand("listTupleStores").getData()) {
			String logicalName = t.getString(0);
			boolean isHistory = t.getBoolean(1);
			boolean isLatest = t.getBoolean(2);
			stores.add(new TupleStore(logicalName, isHistory, isLatest));
		}
		return stores;
	}

	/**
	 * Permanently deletes a named tuple store.
	 * 
	 * @param logicalName
	 *            name of tuple store
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public static void dropTupleStore(String logicalName) throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection("RGMAService");
		connection.addParameter("logicalName", logicalName);
		connection.sendNonResourceCommand("dropTupleStore");
	}

	/**
	 * Returns the value of the version of the RGMA server in use.
	 * 
	 * @return the server version as a string
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public static String getVersion() throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection("RGMAService");
		Tuple t = connection.sendNonResourceCommand("getVersion").getData().get(0);
		return t.getString(0);
	}

	/**
	 * Returns the termination interval that will be applied to all resources.
	 * 
	 * @return the termination interval as a TimeInterval
	 * @throws RGMAPermanentException
	 * @throws RGMATemporaryException
	 */
	public static TimeInterval getTerminationInterval() throws RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection("RGMAService");
		Tuple t = connection.sendNonResourceCommand("getTerminationInterval").getData().get(0);
		return new TimeInterval(t.getInt(0), TimeUnit.SECONDS);
	}
}
