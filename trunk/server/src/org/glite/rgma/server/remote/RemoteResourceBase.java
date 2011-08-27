/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.remote;

import java.net.MalformedURLException;
import java.net.URL;

import org.glite.rgma.server.services.ServerConfig;
import org.glite.rgma.server.services.ServerConstants;
import org.glite.rgma.server.services.consumer.ConsumerService;
import org.glite.rgma.server.services.producer.ondemand.OnDemandProducerService;
import org.glite.rgma.server.services.producer.primary.PrimaryProducerService;
import org.glite.rgma.server.services.producer.secondary.SecondaryProducerService;
import org.glite.rgma.server.services.resource.ResourceManagementService;
import org.glite.rgma.server.servlets.ServletConnection;
import org.glite.rgma.server.servlets.ServletConstants;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.UnknownResourceException;

/**
 * Interface to a remote ResourceServlet
 */
public class RemoteResourceBase {

	protected static PrimaryProducerService s_primaryProducerService;
	protected static SecondaryProducerService s_secondaryProducerService;
	protected static OnDemandProducerService s_onDemandProducerService;
	protected static ConsumerService s_consumerService;

	protected static String s_thisHost;

	protected static final String s_ppPath = "/" + ServerConstants.WEB_APPLICATION_NAME + "/" + ServerConstants.PRIMARY_PRODUCER_SERVICE_NAME;
	protected static final String s_spPath = "/" + ServerConstants.WEB_APPLICATION_NAME + "/" + ServerConstants.SECONDARY_PRODUCER_SERVICE_NAME;
	protected static final String s_odpPath = "/" + ServerConstants.WEB_APPLICATION_NAME + "/" + ServerConstants.ONDEMAND_PRODUCER_SERVICE_NAME;
	private static final String s_cPath = "/" + ServerConstants.WEB_APPLICATION_NAME + "/" + ServerConstants.CONSUMER_SERVICE_NAME;

	public static void setStaticVariables(ResourceManagementService service) throws RGMAPermanentException {
		URL url;
		try {
			url = new URL(service.getURLString());
		} catch (MalformedURLException e) {
			throw new RGMAPermanentException(e);
		}
		String servicePath = url.getPath();
		if (servicePath.equals(s_ppPath)) {
			s_primaryProducerService = (PrimaryProducerService) service;
		} else if (servicePath.equals(s_spPath)) {
			s_secondaryProducerService = (SecondaryProducerService) service;
		} else if (servicePath.equals(s_odpPath)) {
			s_onDemandProducerService = (OnDemandProducerService) service;
		} else if (servicePath.equals(s_cPath)) {
			s_consumerService = (ConsumerService) service;
		} else {
			throw new RGMAPermanentException("RemoteResourceBase called with unexpected path: " + servicePath);
		}

		ServerConfig config = ServerConfig.getInstance();
		s_thisHost = config.getString(ServerConstants.SERVER_HOSTNAME);

	}

	public static void ping(URL url, int resourceId) throws RemoteException, UnknownResourceException, RGMAPermanentException, RGMATemporaryException {

		if (url.getHost().equals(s_thisHost)) {
			String servicePath = url.getPath();
			if (servicePath.equals(s_ppPath)) {
				s_primaryProducerService.ping(resourceId);
			} else if (servicePath.equals(s_spPath)) {
				s_secondaryProducerService.ping(resourceId);
			} else if (servicePath.equals(s_odpPath)) {
				s_onDemandProducerService.ping(resourceId);
			} else if (servicePath.equals(s_cPath)) {
				s_consumerService.ping(resourceId);
			} else {
				throw new RGMAPermanentException("RemoteResourcecalled with invalid path: " + servicePath);
			}
		} else {
			ResourceEndpoint endpoint = new ResourceEndpoint(url, resourceId);
			ServletConnection connection = new ServletConnection(endpoint);
			connection.sendCommandWithUnknowResourceException(ServletConstants.M_PING);
		}
	}
}
