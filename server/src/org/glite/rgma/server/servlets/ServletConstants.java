/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.servlets;

/**
 * Defines constants used by the servlet transport. Those beginning P_ are parameters, M_ are methods, R_ are return
 * values V_ are values of parameters being sent.
 */
public interface ServletConstants {

	/* Names of servlet HTTP parameters. */

	public static final String P_ACTION = "action";

	public static final String P_TABLE_OR_VIEW = "tableOrView";

	public static final String P_CONNECTION_ID = "connectionId";

	public static final String P_RESOURCE_ID = "resourceId";

	public static final String P_TERMINATION_INTERVAL_SEC = "terminationIntervalSec";

	public static final String P_IS_CONTINUOUS = "isContinuous";

	public static final String P_TIMESTAMP = "timeStamp";

	public static final String P_IS_HISTORY = "isHistory";

	public static final String P_IS_LATEST = "isLatest";

	public static final String P_IS_STATIC = "isStatic";

	public static final String P_IS_SECONDARY = "isSecondary";

	public static final String P_SELECT = "select";

	public static final String P_TIME_INTERVAL_SEC = "timeIntervalSec";

	public static final String P_MAX_COUNT = "maxCount";

	public static final String P_TIMEOUT = "timeoutSec";

	public static final String P_DIRECTED_PRODUCER = "producerConnections";

	public static final String P_URL = "url";

	public static final String P_ID = "id";

	public static final String P_DN = "DN";

	public static final String P_FQAN = "FQAN";

	public static final String P_LOGICAL_NAME = "logicalName";

	public static final String P_TABLE_NAME = "tableName";

	public static final String P_INDEX_NAME = "indexName";

	public static final String P_VIEW_NAME = "viewName";

	public static final String P_PREDICATE = "predicate";

	public static final String P_HRP_SEC = "hrpSec";

	public static final String P_INSERT = "insert";

	public static final String P_LRP = "lrpSec";

	public static final String P_QUERY_TYPE = "queryType";

	public static final String P_CONSUMER_URL = "consumerURL";

	public static final String P_CONSUMER_ID = "consumerId";

	public static final String P_STREAMING_URL = "streamingURL";

	public static final String P_STREAMING_PORT = "streamingPort";

	public static final String P_STREAMING_CHUNK_SIZE = "bufferSize";

	public static final String P_STREAMING_PROTOCOL = "streamingProtocol";

	public static final String P_VDB_NAME = "vdbName";

	public static final String P_SCHEMA_LAST_UPDATE_TIMESTAMP = "timeStamp";

	public static final String P_REGISTRY_SERVICE = "registryService";

	public static final String P_REPLICA = "replica";

	public static final String P_SCHEMA_SERVICE = "schemaService";

	public static final String P_CAN_FORWARD = "canForward";

	public static final String P_TABLES = "tables";

	public static final String P_NAME = "name";

	public static final String P_PARAMETER = "parameter";

	public static final String P_VALUE = "value";

	public static final String P_CREATE_TABLE_STATEMENT = "createTableStatement";

	public static final String P_CREATE_INDEX_STATEMENT = "createIndexStatement";

	public static final String P_CREATE_VIEW_STATEMENT = "createViewStatement";

	public static final String P_TABLE_AUTHZ_RULE = "tableAuthz";

	public static final String P_VIEW_AUTHZ_RULE = "viewAuthz";

	public static final String P_TYPE = "type";

	public static final String P_USER_DN = "userDN";
	
	public static final String P_NEW_OWNER_DN = "newOwnerDN";

	public static final String P_CLIENT_HOST_NAME = "clientHostName";

	/* Names of servlet methods. */

	public static final String M_CREATE_ONDEMANDPRODUCER = "createOnDemandProducer";

	public static final String M_CREATE_TABLE = "createTable";

	public static final String M_DROP_TABLE = "dropTable";

	public static final String M_CREATE_INDEX = "createIndex";

	public static final String M_DROP_INDEX = "dropIndex";

	public static final String M_CREATE_VIEW = "createView";

	public static final String M_DROP_VIEW = "dropView";

	public static final String M_GET_ALL_TABLES = "getAllTables";

	public static final String M_GET_ALL_VDBs = "getAllVDBs";

	public static final String M_GET_TABLE_DEFINITION = "getTableDefinition";

	public static final String M_GET_TABLE_INDEXES = "getTableIndexes";

	public static final String M_SET_AUTHZ_RULES = "setAuthorizationRules";
	
	public static final String M_GET_AUTHZ_RULES = "getAuthorizationRules";

	public static final String M_GET_FULL_TABLE_DETAILS = "getFullTableDetails";

	public static final String M_GET_SCHEMA_UPDATES = "getSchemaUpdates";

	public static final String M_ALTER = "alter";

	public static final String M_GET_ALL_SCHEMA = "getAllSchema";

	public static final String M_CREATE_CONSUMER = "createConsumer";

	public static final String M_CREATE_PRIMARYPRODUCER = "createPrimaryProducer";

	public static final String M_CREATE_SECONDARYPRODUCER = "createSecondaryProducer";

	public static final String M_ABORT = "abort";

	public static final String M_HAS_ABORTED = "hasAborted";

	public static final String M_POP = "pop";

	public static final String M_START = "start";

	public static final String M_ADD_PRODUCER = "addProducer";

	public static final String M_REMOVE_PRODUCER = "removeProducer";

	public static final String M_GET_HRP = "getHistoryRetentionPeriod";

	public static final String M_GET_LRP = "getLatestRetentionPeriod";

	public static final String M_DECLARE_TABLE = "declareTable";

	public static final String M_INSERT = "insert";

	public static final String M_CREATE_REGISTRY = "createRegistry";

	public static final String M_CREATE_SCHEMA = "createSchema";

	public static final String M_DESTROY_REGISTRY = "destroyRegistry";

	public static final String M_DESTROY_SCHEMA = "destroySchema";

	public static final String M_GET_ALL_PRODUCERS_FOR_TABLE = "getAllProducersForTable";

	public static final String M_GET_MATCHING_PRODUCERS_FOR_TABLES = "getMatchingProducersForTables";

	public static final String M_REGISTER_PRODUCER_TABLE = "registerProducerTable";

	public static final String M_UNREGISTER_PRODUCER_TABLE = "unregisterProducerTable";

	public static final String M_UNREGISTER_CONTINUOUS_CONSUMER = "unregisterContinuousConsumer";

	public static final String M_ADD_REPLICA = "addReplica";

	public static final String M_PONG = "pong";

	public static final String M_PING = "ping";

	public static final String M_CLOSE = "close";

	public static final String M_DESTROY = "destroy";

	public static final String M_SHOW_SIGN_OF_LIFE = "showSignOfLife";

	public static final String M_GET_TERMINATION_INTERVAL = "getTerminationInterval";

	public static final String M_GET_PROPERTY = "getProperty";

	public static final String M_SET_PROPERTY = "setProperty";

	public static final String M_GET_VERSION = "getVersion";

	public static final String M_DROP_TUPLE_STORE = "dropTupleStore";

	public static final String M_LIST_TUPLE_STORES = "listTupleStores";

}
