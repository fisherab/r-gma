/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.remote;

import java.net.URL;

import org.glite.rgma.server.servlets.ServletConnection;
import org.glite.rgma.server.servlets.ServletConstants;
import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.StreamingProperties;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.Units;
import org.glite.rgma.server.system.UnknownResourceException;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserSystemContext;
import org.glite.voms.FQAN;

/**
 * Interface to a remote ProducerServlet
 */
public class RemoteProducer extends RemoteResourceBase {

	public static TupleSet start(URL url, int resourceId, String select, QueryProperties queryProps, TimeInterval timeout, ResourceEndpoint consumer,
			StreamingProperties streamingProps, UserContext context) throws RemoteException, UnknownResourceException, RGMAPermanentException,
			NumericException, RGMATemporaryException {

		if (url.getHost().equals(s_thisHost)) {
			String servicePath = url.getPath();
			UserSystemContext usContext = new UserSystemContext(context.getDN(), context.getFQANs(), context.getHostName(), s_thisHost.toUpperCase());
			if (servicePath.equals(s_ppPath)) {
				return s_primaryProducerService.start(resourceId, select, queryProps, timeout, consumer, streamingProps, usContext);
			} else if (servicePath.equals(s_spPath)) {
				return s_secondaryProducerService.start(resourceId, select, queryProps, timeout, consumer, streamingProps, usContext);
			} else if (servicePath.equals(s_odpPath)) {
				return s_onDemandProducerService.start(resourceId, select, queryProps, timeout, consumer, streamingProps, usContext);
			} else {
				throw new RGMAPermanentException("RemoteProducer called with invalid path: " + servicePath);
			}
		} else {
			ResourceEndpoint endpoint = new ResourceEndpoint(url, resourceId);
			ServletConnection connection = new ServletConnection(endpoint);

			String queryType = "";
			if (queryProps.isContinuous()) {
				queryType = "continuous";
			} else if (queryProps.isLatest()) {
				queryType = "latest";
			} else if (queryProps.isHistory()) {
				queryType = "history";
			} else if (queryProps.isStatic()) {
				queryType = "static";
			} else {
				throw new RGMAPermanentException("Invalid query properties - not continuous, latest, history or static");
			}

			connection.addParameter(ServletConstants.P_SELECT, select);
			connection.addParameter(ServletConstants.P_QUERY_TYPE, queryType);
			if (queryProps.hasTimeInterval()) {
				connection.addParameter(ServletConstants.P_TIME_INTERVAL_SEC, queryProps.getTimeInterval().getValueAs(Units.SECONDS));
			}
			if (timeout != null) {
				connection.addParameter(ServletConstants.P_TIMEOUT, timeout.getValueAs(Units.SECONDS));
			}
			connection.addParameter(ServletConstants.P_CONSUMER_URL, consumer.getURL().toString());
			connection.addParameter(ServletConstants.P_CONSUMER_ID, consumer.getResourceID());
			connection.addParameter(ServletConstants.P_STREAMING_URL, streamingProps.getStreamingHost());
			connection.addParameter(ServletConstants.P_STREAMING_PORT, streamingProps.getStreamingPort());
			connection.addParameter(ServletConstants.P_STREAMING_CHUNK_SIZE, streamingProps.getChunkSize());
			connection.addParameter(ServletConstants.P_STREAMING_PROTOCOL, streamingProps.getStreamingProtocol());
			connection.addParameter(ServletConstants.P_DN, context.getDN());
			for (FQAN fqan : context.getFQANs()) {
				connection.addParameter(ServletConstants.P_FQAN, fqan.toString());
			}
			return connection.sendCommandWithAllExceptions(ServletConstants.M_START);
		}
	}

	public static void abort(URL url, int resourceId, ResourceEndpoint consumer) throws RemoteException, UnknownResourceException, RGMAPermanentException,
			RGMATemporaryException {
		if (url.getHost().equals(s_thisHost)) {
			String servicePath = url.getPath();
			if (servicePath.equals(s_ppPath)) {
				s_primaryProducerService.abort(resourceId, consumer);
			} else if (servicePath.equals(s_spPath)) {
				s_secondaryProducerService.abort(resourceId, consumer);
			} else if (servicePath.equals(s_odpPath)) {
				s_onDemandProducerService.abort(resourceId, consumer);
			} else {
				throw new RGMAPermanentException("RemoteProducer called with invalid path: " + servicePath);
			}
		} else {
			ResourceEndpoint endpoint = new ResourceEndpoint(url, resourceId);
			ServletConnection connection = new ServletConnection(endpoint);
			connection.addParameter(ServletConstants.P_CONSUMER_URL, consumer.getURL().toString());
			connection.addParameter(ServletConstants.P_CONSUMER_ID, consumer.getResourceID());
			connection.sendCommandWithUnknowResourceException(ServletConstants.M_ABORT);
		}
	}
}
