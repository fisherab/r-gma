/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services;

/**
 * Common Server constants
 */
public class ServerConstants {

	// The top section of this file contains 'Constant' constants grouped together by component type
	// The bottom section contains constants to be obtained from the configuration file, again
	// grouped together by component type
	// Please ensure that all constants are of the form 'componentName.property'

	//
	// Constant constants (as opposed to property names)
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	//

	/** Name of the RGMA_HOME system property */
	public static final String RGMA_HOME_PROPERTY = "RGMA_HOME";

	/** location of the server configuration file relative to RGMA_HOME */
	public static final String SERVER_CONFIG_LOCATION = "/etc/rgma-server/rgma-server.props";

	// Logging constants

	/** location of the server configuration file relative to RGMA_HOME */
	public static final String LOG4J_LOCATION = "/etc/rgma-server/log4j.properties";

	/** Interval between reading the log4j.properties file */
	public static final long LOG4J_INTERVAL_MILLIS = 60000;

	/** logger used for control messages - service startup, closedown and configuration */
	public static final String CONTROL_LOGGER = "control";

	/** logger used for security messages */
	public static final String SECURITY_LOGGER = "security";

	/** The class logger has not been initialised. */
	public static final String UNDEFINED_LOG_LEVEL = "UNDEFINED";

	public static final String PROPERTY_LOGGING_LEVEL = "LoggingLevel";

	// Protocol constants

	/** Secure service URL prefix */
	public static final String SERVICE_SECURE_URL_PREFIX = "https://";

	// Servlet Name constants

	/** service URL suffix */
	public static final String WEB_APPLICATION_NAME = "R-GMA";

	public static final String CONSUMER_SERVICE_NAME = "ConsumerServlet";

	public static final String ONDEMAND_PRODUCER_SERVICE_NAME = "OnDemandProducerServlet";

	public static final String PRIMARY_PRODUCER_SERVICE_NAME = "PrimaryProducerServlet";

	public static final String REGISTRY_SERVICE_NAME = "RegistryServlet";

	public static final String SCHEMA_SERVICE_NAME = "SchemaServlet";

	public static final String SECONDARY_PRODUCER_SERVICE_NAME = "SecondaryProducerServlet";

	// QueryType Constants

	public static final String CONTINUOUS = "continuous";

	public static final String LATEST = "latest";

	public static final String HISTORY = "history";

	public static final String STATIC = "static";

	// Database Types constants

	/** MySQL. */
	public static final String MYSQL_DB_TYPE = "mysql";

	/** Oracle. */
	public static final String ORACLE_DB_TYPE = "oracle";

	/** hyper sonic. */
	public static final String HSQL_DB_TYPE = "hsql";

	// Memory Database Constants - Constant Constants

	/** The name of the property which will define the database username. */
	public static final String MEMORY_DATABASE_USERNAME = "sa";

	/** The name of the property which will define the database password. */
	public static final String MEMORY_DATABASE_PASSWORD = "";

	// Resource Constants - Constant Constants

	/** Property name of ID in resource id file. */
	public static final String RESOURCE_ID = "resource.id";

	// TaskManager Properties - Constant Constants

	/** The number of attempt for each queued task. NB this does not include running tasks. */
	public static final String TASKMANAGER_TASK_ATTEMPTS = "taskmanager.attempts";

	/** A list of all the good keys. */
	public static final String TASKMANAGER_TASK_GOODKEYS = "taskmanager.goodKeys";

	/** A list of all task keys not registered as good keys for queued and running tasks. */
	public static final String TASKMANAGER_TASK_NOT_GOODKEYS = "taskmanager.notGoodKeys";

	/** The number of tasks per key for queued and running tasks. */
	public static final String TASKMANAGER_TASKS_PER_KEY = "taskmanager.tasksPerKey";

	public static final String TASKMANAGER_SUMMARY = "taskManagerSummary";

	// Service Properties - Constant Constants

	/** Service/resource details. */
	public static final String SERVICE_STATUS_DETAILS = "serviceStatusDetails";

	/** Service status. */
	public static final String SERVICE_STATUS = "serviceStatus";

	/** Service/resource list. */
	public static final String SERVICE_RESOURCES = "resources";

	/** Service/resource status. */
	public static final String RESOURCE_STATUS = "resourceStatus";

	/** Service/resource status. */
	public static final String RESOURCE_TABLE_STATUS = "resourceTableStatus";

	public static final String RESOURCE_HOST_STATUS = "resourceHostStatus";

	/** Service/streaming status. */
	public static final String STREAMING_STATUS = "streamingStatus";

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	//
	// Constants - Configuration Parameters
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	//

	// Consumer Properties - Configuration Parameters

	/** Time taken in seconds before the schema is checked for table modifications. */
	public static final String CONSUMER_SCHEMA_CHECK_INTERVAL_SECS = "consumer.schemaCheckIntervalSecs";

	/** Maximum time in seconds for any consumer task. */
	public static final String CONSUMER_MAXIMUM_TASK_TIME_SECS = "consumer.maxTaskTimeSecs";

	/**
	 * Maximum number of tuples in the consumer's memory queue. It is best for this to be a multiple of
	 * CONSUMER_TUPLE_WRITE_BATCH_SIZE
	 */
	public static final String CONSUMER_MAX_TUPLES_MEM = "consumer.maxTuplesMem";

	/** Maximum number of tuples in the consumer's DB queue. */
	public static final String CONSUMER_MAX_TUPLES_DB = "consumer.maxTuplesDB";

	/**
	 * Number of tuples to write to the TupleQueue database in one operation. 50 is a good number for MySQL.
	 */
	public static final String CONSUMER_TUPLE_WRITE_BATCH_SIZE = "consumer.tupleWriteBatchSize";

	/** Maximum number of tuples per streaming chunk. */
	public static final String CONSUMER_MAX_TUPLE_COUNT_PER_STREAMED_CHUNK = "consumer.maxTupleCountPerStreamedChunk";

	/** How often to check streaming producers are still alive in seconds. */
	public static final String CONSUMER_PING_INTERVAL_SECS = "consumer.pingIntervalSecs";

	/** Maximum number of tuples a consumer can pop each time. */
	public static final String CONSUMER_MAX_POP_TUPLES = "consumer.maxPopTuplesCount";

	/** Name of the file used to store the next available resource ID. */
	public static final String CONSUMER_ID_FILE = "consumer.idFile";

	// Database Properties - Configuration Parameters

	/** The name of the property which will define the URL location of the database. */
	public static final String DATABASE_LOCATION_URL = "database.location.url";

	/** The name of the property which will define the database username. */
	public static final String DATABASE_USERNAME = "database.username";

	/** The name of the property which will define the database password. */
	public static final String DATABASE_PASSWORD = "database.password";

	/** The name of the property which will define the jdbc connection driver. */
	public static final String DATABASE_JDBC_DRIVER = "database.jdbc.driver";

	/** Database type - not case sensitive. */
	public static final String DATABASE_TYPE = "database.type";

	/**
	 * The name of the property which will define the maximum number of connections the pool will handle.
	 */
	public static final String DATABASE_CONNECTION_POOL_MAX_ACTIVE = "database.connection.pool.maxActive";

	/**
	 * The name of the property which will define the maximum number of idle connections the pool will have.
	 */
	public static final String DATABASE_CONNECTION_POOL_MAX_IDLE = "database.connection.pool.maxIdle";

	/**
	 * The name of the property which will define the maximum time to wait for a connection before throwing an exception
	 * -1 means wait indefinitely.
	 */
	public static final String DATABASE_CONNECTION_POOL_MAX_WAIT_SECS = "database.connection.pool.maxWait.secs";

	/** The width of the security log produced of database accesses */
	public static final String DATABASE_LOG_WIDTH = "database.log.width";

	// Memory Database Properties - Configuration Parameters

	/** The name of the property which will define the URL location of the database. */
	public static final String MEMORY_DATABASE_LOCATION_URL = "memory.database.location.url";

	/** The name of the property which will define the jdbc connection driver. */
	public static final String MEMORY_DATABASE_JDBC_DRIVER = "memory.database.jdbc.driver";

	// Primary Producer Properties - Configuration Parameters

	/** Name of the file used to store the next available resource ID. */
	public static final String PRIMARY_PRODUCER_ID_FILE = "primaryproducer.idFile";

	/** Interval for cleaning up waiting producer for consumer * */
	public static final String PRIMARY_PRODUCER_CLEANUP_INTERVAL_SECS = "primaryproducer.cleanupIntervalSecs";

	// Secondary Producer Properties - Configuration Parameters

	/** Name of the file used to store the next available resource ID. */
	public static final String SECONDARY_PRODUCER_ID_FILE = "secondaryproducer.idFile";

	/** Indicates the Interval(No of inserted tuples) between Memory Check */
	public static final String SECONDARY_PRODUCER_COUNT_OF_TUPLES_BETWEEN_MEMORY_CHECKS = "secondaryproducer.countOfTuplesBetweenMemoryChecks";

	// OnDemand Producer Properies
	/** Name of the file used to store the next available resource ID. */
	public static final String ONDEMAND_PRODUCER_ID_FILE = "ondemandproducer.idFile";

	// Producer Properties - Configuration Parameters

	/** Maximum number of tuples to send in each streaming chunk. */
	public static final String PRODUCER_MAX_TUPLE_COUNT_PER_STREAMED_CHUNK = "producer.maxTupleCountPerStreamedChunk";

	// Registry Properties - Configuration Parameters

	/** Registry cleanup thread interval time to wait in seconds. */
	public static final String REGISTRY_CLEANUPTHREAD_INTERVAL_SECS = "registry.cleanupthread.interval.secs";

	/** Registry replication thread interval time to wait in seconds. */
	public static final String REGISTRY_REPLICATION_INTERVAL_SECS = "registry.replication.interval.secs";

	/** Maximum time a replication task should take before it is aborted. */
	public static final String REGISTRY_REPLICATION_MAX_TASK_TIME_SECS = "registry.replication.max.task.time.secs";

	/**
	 * The amount of seconds to be added to the last contact time during replication to compensate for the time taken to
	 * replicate
	 */
	public static final String REGISTRY_REPLICATION_LAG_SECS = "registry.replication.lag";

	// Resource Properties - Configuration Parameters

	/**
	 * Resource termination interval (in seconds). It should not normally be around 15 minutes (900 seconds).
	 */
	public static final String RESOURCE_TERM_INTERVAL_SECS = "resource.termIntervalSecs";

	/**
	 * Worse case time for passing update message to the registry for the resource manager in seconds. This must be less
	 * than the RESOURCE_MIN_TERM_INTERVAL_SECS and should not normally be less than 5 minutes (300 seconds).
	 */
	public static final String RESOURCE_REGISTRY_LATENCY_SECS = "resource.registryLatencySecs";

	/**
	 * Interval in seconds (5 minutes perhaps) for checking the state of the resources and marking as closed
	 */
	public static final String RESOURCE_LOCAL_UPDATE_INTERVAL_SECS = "resource.localUpdateIntervalSecs";

	/**
	 * Interval in seconds (30 minutes perhaps) for checking to the state of the resources and removing from the manager
	 * and update the registry
	 */
	public static final String RESOURCE_REMOTE_UPDATE_INTERVAL_SECS = "resource.remoteUpdateIntervalSecs";

	/** Interval between recording resource id numbers (10 seems a reasonable number). */
	public static final String RESOURCE_ID_RECORDING_INTERVAL_COUNT = "resource.idRecordingIntervalCount";

	/** Maximum number of tries for a task call. */
	public static final String RESOURCE_MAXIMUM_TASK_ATTEMPT_COUNT = "resource.maxTaskAttemptCount";

	/**
	 * If remote expection received for more than this time when pinging a remote resource treat the resource as being
	 * no longer present.
	 */
	public static final String RESOURCE_INTERVAL_TO_GIVE_UP_ON_UNREACHABLE_SECS = "resource.intervalToGiveUpOnUnreachableSecs";

	// Schema Properties - Configuration Parameters

	/** Schema replication interval. */
	public static final String SCHEMA_REPLICATIONTHREAD_INTERVAL_SECS = "schema.replicationIntervalSecs";

	/** Maximum time a replication task should take before it is aborted. */
	public static final String SCHEMA_REPLICATION_MAX_TASK_TIME_SECS = "schema.replicationMaxTaskTimeSecs";

	// Service Properties - Configuration Parameters

	/** The location of the file which defines what version this server is. */
	public static final String SERVER_VERSION_FILE_LOCATION = "server.version.file.location";

	/** Server hostname. */
	public static final String SERVER_HOSTNAME = "server.hostname";

	/** Server port. */
	public static final String SERVER_PORT = "server.port";

	/** Name of memory pool to watch for running low on memory */
	public static final String SERVER_POOL_TO_WATCH = "server.poolToWatch";

	/** Bytes of memory to keep free on the HEAP */
	public static final String SERVER_MAX_HEAD_ROOM = "server.maxHeadRoom";

	/** The files with a list of glob patterns of clients allowed access */
	public static final String SERVER_ALLOWED_CLIENT_HOSTNAME_PATTERNS_FILE = "server.allowed.client.hostname.patterns.file";

	/** Interval in seconds between checking the files of allowed client patterns */
	public static final String SERVER_ALLOWED_CLIENT_CHECK_INTERVAL_SECS = "server.client.access.configuration.check.interval.secs";

	/** Maximum expected response time for a service call in milliseconds (integer) */
	public static final String SERVER_MAXIMUM_EXPECTED_RESPONSE_TIME_MILLIS = "server.maximumExpectedResponseTimeMillis";

	/** Maximum simultaneous requests to be handled before server is considered busy */
	public static final String SERVER_MAXIMUM_REQUEST_COUNT = "server.maximumRequestCount";

	// ServletConnection Properties - Configuration Parameters

	/** The location of the X509_USER_PROXY */
	public static final String SERVLETCONNECTION_X509_USER_PROXY = "servletconnection.X509_USER_PROXY";

	/** The location of the X509_CERT_DIR */
	public static final String SERVLETCONNECTION_X509_CERT_DIR = "servletconnection.X509_CERT_DIR";

	// StreamingReceiver Properties - Configuration Parameters

	/** The frequency to check for and cleanup dead RunningReplies. */
	public static final String STREAMING_RECEIVER_CLEANUP_INTERVAL_SECS = "streamingreceiver.cleanupIntervalSecs";

	/** Port number for the streaming receiver to listen on. */
	public static final String STREAMING_RECEIVER_PORT = "streamingreceiver.port";

	// StreamingSender Properties - Configuration Parameters

	/** The frequency to check for and cleanup StreamingSources. */
	public static final String STREAMING_SENDER_CLEANUP_INTERVAL_SECS = "streamingsender.cleanupIntervalSecs";

	/** Optimal NIO packet size in bytes. */
	public static final String STREAMING_SENDER_OPTIMAL_PACKET_SIZE_BYTES = "streamingsender.optimalPacketSizeBytes";

	/** How long to keep a source alive when it has no queries (seconds as integer). */
	public static final String STREAMING_SENDER_PERIOD_TO_KEEP_REDUNDANT_SOURCE_SECS = "streamingsender.periodToKeepRedundantSource";

	// Streaming Properties

	/** Set True to use direct buffers for I/O' */
	public static final String STREAMING_ALLOCATE_DIRECT = "streaming.allocateDirect";

	// TaskManager Properties - Configuration Parameters

	/** The number TaskInvocators, one TaskInvocators per thread. */
	public static final String TASKMANAGER_THREADS_IN_POOL = "taskmanager.threadsInPool";

	/** The number of TaskInvocators that will only process tasks with good keys. */
	public static final String TASKMANAGER_GOOD_ONLY_THREADS = "taskmanager.goodOnlyThreads";

	/** The frequency to check for hung TaskInvocators. */
	public static final String TASKMANAGER_HANGING_INVOCATORS_CHECK_PERIOD_SECS = "taskmanager.hangingInvocatorsCheckPeriodSecs";

	/**
	 * The period after which a task should have finished, that the TaskInvocator is considered to have hung.
	 */
	public static final String TASKMANAGER_HANGING_INVOCATORS_CHECK_DELAY_SECS = "taskmanager.hangingInvocatorsCheckDelaySecs";

	/** The maximum number of tasks that are queued and would run if there were a slot. */
	public static final String TASKMANAGER_MAXIMUM_GOOD_QUEUED_TASK_COUNT = "taskmanager.maximumGoodQueuedTaskCount";

	// TupleStoreManager Properties - Configuration Parameters

	/** How often the TupleStoreManager runs the tuple cleanup operation for DB storage */
	public static final String TUPLESTOREMANAGER_DB_CLEANUP_INTERVAL_SECS = "tuplestoremanager.db.cleanupIntervalSecs";

	/** How often the TupleStoreManager runs the tuple cleanup operation for MEM storage */
	public static final String TUPLESTOREMANAGER_MEM_CLEANUP_INTERVAL_SECS = "tuplestoremanager.mem.cleanupIntervalSecs";

	/** Maximum number of tuples to he held in a history tuple store for DB storage */
	public static final String TUPLESTOREMANAGER_DB_MAX_HISTORY_TUPLES = "tuplestoremanager.db.maxHistoryTuples";

	/** Maximum number of tuples to he held in a history tuple store for MEM storage */
	public static final String TUPLESTOREMANAGER_MEM_MAX_HISTORY_TUPLES = "tuplestoremanager.mem.maxHistoryTuples";

	// VDB Properties - Configuration Parameters

	/** Directory with VDB definition files */
	public static final String VDB_CONFIGURATION_DIRECTORY = "vdb.configuration.directory";

	/** Interval between checking for changes to the VDB definition files */
	public static final String VDB_CONFIGURATION_CHECK_INTERVAL_SECS = "vdb.configuration.check.interval.secs";
}