/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.remote;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.glite.rgma.server.servlets.ServletConnection;
import org.glite.rgma.server.servlets.ServletConstants;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.RemoteException;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UserContext;
import org.glite.rgma.server.system.UserContextInterface;

/**
 * Interface to a remote SchemaServlet
 */
public class RemoteSchema implements ServletConstants {
	public static boolean alter(URL url, String vdbName, String torv, String tableName, String action, String name, String type, UserContext userContext)
			throws RGMAPermanentException, RGMATemporaryException, RemoteException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdbName);
		connection.addParameter(P_CAN_FORWARD, false);
		connection.addParameter(P_TABLE_OR_VIEW, torv);
		connection.addParameter(P_TABLE_NAME, tableName);
		connection.addParameter(P_ACTION, action);
		connection.addParameter(P_NAME, name);
		if (type != null) {
			connection.addParameter(P_TYPE, type);
		}
		connection.addParameter(P_USER_DN, userContext.getDN());
		connection.addParameter(P_CLIENT_HOST_NAME, userContext.getHostName());
		return convertToBoolean(connection.sendCommand(M_ALTER));
	}

	public static boolean createIndex(URL url, String vdb, String createIndexStatement, UserContextInterface userContext) throws RemoteException,
			RGMAPermanentException, RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		connection.addParameter(P_CAN_FORWARD, false);
		connection.addParameter(P_CREATE_INDEX_STATEMENT, createIndexStatement);
		connection.addParameter(P_USER_DN, userContext.getDN());
		connection.addParameter(P_CLIENT_HOST_NAME, userContext.getHostName());
		return convertToBoolean(connection.sendCommand(M_CREATE_INDEX));
	}

	public static boolean createTable(URL url, String vdb, String createTableStatement, List<String> tableAuthz, UserContextInterface requestContext)
			throws RemoteException, RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		connection.addParameter(P_CAN_FORWARD, false);
		connection.addParameter(P_CREATE_TABLE_STATEMENT, createTableStatement);
		connection.addParameter(P_USER_DN, requestContext.getDN());
		connection.addParameter(P_CLIENT_HOST_NAME, requestContext.getHostName());
		for (int i = 0; i < tableAuthz.size(); i++) {
			connection.addParameter(P_TABLE_AUTHZ_RULE, tableAuthz.get(i));
		}
		return convertToBoolean(connection.sendCommand(M_CREATE_TABLE));
	}

	public static boolean createView(URL url, String vdb, String createViewStatement, List<String> viewAuthz, UserContextInterface userContext)
			throws RemoteException, RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		connection.addParameter(P_CAN_FORWARD, false);
		connection.addParameter(P_CREATE_VIEW_STATEMENT, createViewStatement);
		connection.addParameter(P_USER_DN, userContext.getDN());
		connection.addParameter(P_CLIENT_HOST_NAME, userContext.getHostName());
		for (int i = 0; i < viewAuthz.size(); i++) {
			connection.addParameter(P_VIEW_AUTHZ_RULE, viewAuthz.get(i));
		}
		return convertToBoolean(connection.sendCommand(M_CREATE_VIEW));
	}

	public static boolean dropIndex(URL url, String vdb, String tableName, String indexName, UserContextInterface userContext) throws RemoteException,
			RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		connection.addParameter(P_TABLE_NAME, tableName);
		connection.addParameter(P_CAN_FORWARD, false);
		connection.addParameter(P_INDEX_NAME, indexName);
		connection.addParameter(P_USER_DN, userContext.getDN());
		connection.addParameter(P_CLIENT_HOST_NAME, userContext.getHostName());
		return convertToBoolean(connection.sendCommand(M_DROP_INDEX));
	}

	public static boolean dropTable(URL url, String vdb, String tableName, UserContextInterface requestContext) throws RemoteException, RGMAPermanentException,
			RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		connection.addParameter(P_CAN_FORWARD, false);
		connection.addParameter(P_TABLE_NAME, tableName);
		connection.addParameter(P_USER_DN, requestContext.getDN());
		connection.addParameter(P_CLIENT_HOST_NAME, requestContext.getHostName());
		return convertToBoolean(connection.sendCommand(M_DROP_TABLE));
	}

	public static boolean dropView(URL url, String vdb, String viewName, UserContextInterface userContext) throws RemoteException, RGMAPermanentException,
			RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		connection.addParameter(P_CAN_FORWARD, false);
		connection.addParameter(P_VIEW_NAME, viewName);
		connection.addParameter(P_USER_DN, userContext.getDN());
		connection.addParameter(P_CLIENT_HOST_NAME, userContext.getHostName());
		return convertToBoolean(connection.sendCommand(M_DROP_VIEW));
	}

	public static List<TupleSet> getAllSchema(URL url, String vdb) throws RemoteException, RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		return new ArrayList<TupleSet>(connection.sendCommandForMultipleResultSets(M_GET_ALL_SCHEMA));
	}

	public static List<TupleSet> getSchemaUpdates(URL url, String vdb, long timeST) throws RemoteException, RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		connection.addParameter(P_SCHEMA_LAST_UPDATE_TIMESTAMP, timeST);
		return new ArrayList<TupleSet>(connection.sendCommandForMultipleResultSets(M_GET_SCHEMA_UPDATES));
	}

	public static String getVersion(URL url, String vdb) throws RemoteException, RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		TupleSet rs = connection.sendCommand(M_GET_VERSION);
		return rs.getData().get(0)[0];
	}

	public static void ping(URL url, String vdb) throws RemoteException, RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.sendCommand(M_PING);
	}

	public static boolean setAuthorizationRules(URL url, String vdb, String tableName, List<String> tableAuthz, UserContextInterface userContext)
			throws RemoteException, RGMAPermanentException, RGMATemporaryException {
		ServletConnection connection = new ServletConnection(url);
		connection.addParameter(P_VDB_NAME, vdb);
		connection.addParameter(P_CAN_FORWARD, false);
		connection.addParameter(P_TABLE_NAME, tableName);
		connection.addParameter(P_USER_DN, userContext.getDN());
		connection.addParameter(P_CLIENT_HOST_NAME, userContext.getHostName());
		for (int i = 0; i < tableAuthz.size(); i++) {
			connection.addParameter(P_TABLE_AUTHZ_RULE, tableAuthz.get(i));
		}
		return convertToBoolean(connection.sendCommand(M_SET_AUTHZ_RULES));
	}

	private static boolean convertToBoolean(TupleSet rs) throws RGMAPermanentException {
		return Boolean.parseBoolean(rs.getData().get(0)[0]);
	}

}
