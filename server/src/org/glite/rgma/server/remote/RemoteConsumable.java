/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.remote;

import java.net.URL;

import org.glite.rgma.server.servlets.ServletConnection;
import org.glite.rgma.server.servlets.ServletConstants;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.UnknownResourceException;

/**
 * Interface to a remote ConsumerServlet
 */
public class RemoteConsumable extends RemoteResourceBase {

	public static void addProducer(URL url, int resourceID, ProducerTableEntry producerTable) throws RemoteException, UnknownResourceException,
			RGMAPermanentException, RGMATemporaryException {

		if (url.getHost().equals(s_thisHost)) {
			if (url.getPath().indexOf("SecondaryProducer") >= 0) {
				s_secondaryProducerService.addProducer(resourceID, producerTable);
			} else {
				s_consumerService.addProducer(resourceID, producerTable);
			}
		} else {
			ResourceEndpoint endpoint = new ResourceEndpoint(url, resourceID);
			ServletConnection connection = new ServletConnection(endpoint);
			ResourceEndpoint producerEndpoint = producerTable.getEndpoint();
			ProducerType type = producerTable.getProducerType();

			connection.addParameter(ServletConstants.P_URL, producerEndpoint.getURL().toString());
			connection.addParameter(ServletConstants.P_ID, producerEndpoint.getResourceID());
			connection.addParameter(ServletConstants.P_VDB_NAME, producerTable.getVdbName());
			connection.addParameter(ServletConstants.P_TABLE_NAME, producerTable.getTableName());
			connection.addParameter(ServletConstants.P_IS_LATEST, type.isLatest());
			connection.addParameter(ServletConstants.P_IS_HISTORY, type.isHistory());
			connection.addParameter(ServletConstants.P_IS_STATIC, type.isStatic());
			connection.addParameter(ServletConstants.P_IS_CONTINUOUS, type.isContinuous());
			connection.addParameter(ServletConstants.P_IS_SECONDARY, type.isSecondary());
			connection.addParameter(ServletConstants.P_HRP_SEC, producerTable.getHistoryRetentionPeriod());
			connection.addParameter(ServletConstants.P_PREDICATE, producerTable.getPredicate());
			connection.sendCommandWithUnknowResourceException(ServletConstants.M_ADD_PRODUCER);
		}
	}

	public static void removeProducer(URL url, int resourceID, ResourceEndpoint producer) throws RemoteException, UnknownResourceException,
			RGMAPermanentException, RGMATemporaryException {

		if (url.getHost().equals(s_thisHost)) {
			if (url.getPath().indexOf("SecondaryProducer") >= 0) {
				s_secondaryProducerService.removeProducer(resourceID, producer);
			} else {
				s_consumerService.removeProducer(resourceID, producer);
			}
		} else {
			ResourceEndpoint endpoint = new ResourceEndpoint(url, resourceID);
			ServletConnection connection = new ServletConnection(endpoint);
			connection.addParameter(ServletConstants.P_URL, producer.getURL().toString());
			connection.addParameter(ServletConstants.P_ID, producer.getResourceID());
			connection.sendCommandWithUnknowResourceException(ServletConstants.M_REMOVE_PRODUCER);
		}
	}
}
