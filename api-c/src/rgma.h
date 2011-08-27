/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

#ifndef RGMA_H
#define RGMA_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>  /* for FILE */

/* enums */

/**  Parameter used when creating a Consumer. */
typedef enum {
    /** Continuous query type. */
    RGMAQueryType_C,
    /** History query type. */
    RGMAQueryType_H,
    /** Latest query type. */
    RGMAQueryType_L,
    /** Static query type. */
    RGMAQueryType_S,
    /** Continuous query type with a query interval. */
    RGMAQueryTypeWithInterval_C,
    /** History query type with a query interval. */
    RGMAQueryTypeWithInterval_H,
    /** Latest query type with a query interval. */
    RGMAQueryTypeWithInterval_L
} RGMAQueryType;

/**  Parameter used to specify storage when creating a PrimaryProducer or SecondaryProducer. */
typedef enum {
    /** Memory storage type. */
    RGMAStorageType_MEMORY,
    /** Database storage type. */
    RGMAStorageType_DATABASE
} RGMAStorageType;

/**  Parameter used to specify the type of queries that a PrimaryProducer or SecondaryProducer will support. */
typedef enum {
    /** Continuous query */
    RGMASupportedQueries_C,
    /** Continuous and history query */
    RGMASupportedQueries_CH,
    /**  Continuous and latest query */
    RGMASupportedQueries_CL,
    /**  Continuous, history and latest query */
    RGMASupportedQueries_CHL
} RGMASupportedQueries;

/**  Parameter used to specify the type of columns. It used as part of an RGMAColumnDefinition */
typedef enum {
    /** INTEGER data type. */
    RGMAColumnType_INTEGER,
    /**  REAL data type. */
    RGMAColumnType_REAL,
    /**  DOUBLE data type. */
    RGMAColumnType_DOUBLE,
    /**  TIMESTAMP data type. */
    RGMAColumnType_TIMESTAMP,
    /**  CHAR data type. */
    RGMAColumnType_CHAR,
    /** VARCHAR data type. */
    RGMAColumnType_VARCHAR,
    /**  DATE data type. */
    RGMAColumnType_DATE,
    /**  TIME data type. */
    RGMAColumnType_TIME
} RGMAColumnType;

/**  Parameter used to identify the type of an RGMAException. */
typedef enum {
    /**  May work if call repeated. */
    RGMAExceptionType_TEMPORARY,
    /**  Unlikely to work if call repeated. */
    RGMAExceptionType_PERMANENT
} RGMAExceptionType;

/* structs */

/** The RGMAPrimaryProducer structure is an opaque structure representing a single PrimaryProducer.
 */
typedef struct RGMAPrimaryProducer_S RGMAPrimaryProducer;

/** The RGMASecondaryProducer structure is an opaque structure representing a single SecondaryProducer.
 */
typedef struct RGMASecondaryProducer_S RGMASecondaryProducer;

/** The RGMAOnDemandProducer structure is an opaque structure representing a single OnDemandProducer.
 */
typedef struct RGMAOnDemandProducer_S RGMAOnDemandProducer;

/** The RGMAConsumer structure is an opaque structure representing a single Consumer.
 */
typedef struct RGMAConsumer_S RGMAConsumer;

/** The RGMAResourceEndpoint structure represents a resource-endpoint for an
 *  R-GMA resource (consumer or producer instance). You can obtain a copy
 *  of the resource-endpoint for a resource by calling RGMA_getEndpoint.
 *  You should free the memory associated with the resource-endpoint using
 *  RGMA_freeResourceEndpoint, when you have finished with it.
 */
typedef struct {
    /** Resource ID. */
    int resourceId;
    /** Resource URL. */
    char *url;
} RGMAResourceEndpoint;

/** The RGMAException structure holds an R-GMA exception. The last parameter to most calls is a pointer
 * to a pointer to an RGMAException. The user should declare a pointer to an RGMAException and then pass a pointer to this.
 * If the function detects an error an RGMAException will be created. To detect that an error has occurred examine the pointer and
 * if it is not NULL this signifies that an error has occurred.
 *
 *  The errorType field will be:
 *
 *  o RGMAExceptionType_TEMPORARY   - may work if you try again \n
 *  o RGMAExceptionType_PERMANENT   - not expected to work if you try again
 *
 *  The errorMessage field will describe the error, and will never be NULL.
 *
 *  The numSuccessfulOps field shows the number of operations that completed
 *  successfully before the exception was raised. It will only be non-zero in
 *  batch operations that have partially succeeded - currently, this means just
 *  RGMAPrimaryProducer_insertList().
 *
 * It is important to check for errors after each call and then to free the error by calling
 *  RGMA_freeException(). It is not necessary to set the pointer to the exception to NULL.
 */
typedef struct {
    /** Exception type. */
    RGMAExceptionType type;
    /** Error message. */
    char *message;
    /** Number of operations that completed successfully. */
    int numSuccessfulOps;
} RGMAException;

/** The RGMAStringList structure holds a (possibly empty) list of strings.
 *
 *  It is allocated on the heap and you should call RGMA_freeStringList to
 *  free it when it is no longer required.
 */
typedef struct {
    /** Number of strings in the list. */
    int numStrings;
    /** List of strings. */
    char **string;
} RGMAStringList;

/** The RGMARow structure holds a single row in an RGMATupleSet. A NULL pointer is used
 * to represent a database NULL value.
 */
typedef struct {
    /** Array of column values (INDIVIDUAL VALUES MAY BE NULL). */
    char **cols;
} RGMATuple;

/** The RGMATupleSet structure holds the set of tuples
 *  returned by a consumer query (RGMAConsumer_pop).
 *
 *  In an empty result set, the number of rows and columns will be zero.
 *
 *  There is a warning field which may be an empty string but will never be NULL.
 *
 *  The endOfResults flag in the result set is set to 1 (true) when there are
 *  no more tuples to be returned for a query. Otherwise it will be 0 (false).
 *
 *  The result set is allocated on the heap, and you should free it by calling
 *  RGMA_freeTupleSet when it is no longer required. This will also free up all the Tuples.
 *  You can also detach parts
 *  of a tuple set for use in your own code, by changing the corresponding
 *  pointer in the tuple set to NULL (RGMA_freeTupleSet safely skips over
 *  NULL pointers in any part of a tuple set).
 */
typedef struct {
    /** Number of tuples. */
    int numTuples;
    /** Number of columns. */
    int numCols;
    /** Array of rows. */
    RGMATuple *tuples;
    /** Warning message - it may be an empty string. */
    char *warning;
    /** End-of-results flag. */
    int isEndOfResults;
} RGMATupleSet;

/** The RGMATupleStore structure holds details of a user's permanent tuple stores. */
typedef struct {
    /** Logical name. */
    char *logicalName;
    /** true if history supported */
    int isHistory;
    /** true if latest supported */
    int isLatest;
} RGMATupleStore;

/** The RGMATupleStoreList structure holds a list of RGMATupleStore structures.
 *  It may be empty.
 *
 *  It is allocated on the heap and you should call RGMA_freeTupleStoreList to
 *  free it when it is no longer required.
 */
typedef struct {
    /** Length of list. */
    int numTupleStores;
    /** List of tuple stores. */
    RGMATupleStore *tupleStore;
} RGMATupleStoreList;

/** The RGMAProducerTableEntry structure holds details of a producer resource's
 *  registry entry for a specified table. */
typedef struct {
    /** Producer resource's endpoint. */
    RGMAResourceEndpoint *endpoint;
    /** 1 if secondary producer else 0 */
    int isSecondary;
    /** 1 if continuous queries supported else 0 */
    int isContinuous;
    /** 1 if static queries supported else 0 */
    int isStatic;
    /** 1 if history queries supported else 0 */
    int isHistory;
    /** 1 if latest queries supported else 0 */
    int isLatest;
    /** predicate associated with the table */
    char* predicate;
    /** History Retention Period (in seconds) for this table. */
    int hrpSec;
} RGMAProducerTableEntry;

/** The RGMAProducerTableEntryList structure holds a list of
 *  RGMAProducerTableEntry structures. It may be empty.
 *
 *  It is allocated on the heap and you should call
 *  RGMA_freeProducerTableEntryList to free it when it is no longer required.
 */
typedef struct {
    /** Length of list. */
    int numProducerTableEntries;
    /** List of producer/table entries. */
    RGMAProducerTableEntry *producerTableEntry;
} RGMAProducerTableEntryList;

/** The RGMAIndex structure holds the definition of a single table index. */
typedef struct {
    /** Index name. */
    char *indexName;
    /** Number of columns in index. */
    int numCols;
    /** List of columns in index. */
    char **column;
} RGMAIndex;

/** The RGMAIndexList structure holds a list of RGMAIndex structures.
 *  It may be empty.
 *
 *  It is allocated on the heap and you should call RGMA_freeIndexList to
 *  free it when it is no longer required.
 */
typedef struct {
    /** Length of list. */
    int numIndexes;
    /** List of tuple stores. */
    RGMAIndex *index;
} RGMAIndexList;

/** The RGMAColumnDefinition structure holds the definition of a single column.
 *
 *  Possible column types are: RGMAColumnType_INTEGER,
 *  RGMAColumnType_REAL, RGMAColumnType_DOUBLE, RGMAColumnType_TIMESTAMP,
 *  RGMAColumnType_CHAR, RGMAColumnType_VARCHAR, RGMAColumnType_DATE and
 *  RGMAColumnType_TIME.
 */
typedef struct {
    /** Column name. */
    char *name;
    /** Column type. */
    RGMAColumnType type;
    /** Column width (used with TIME, TIMESTAMP, CHAR and VARCHAR only). */
    int size;
    /** NOT NULL flag (1=true, 0=false). */
    int notNull;
    /** NOT NULL flag (1=true, 0=false). */
    int primaryKey;
} RGMAColumnDefinition;

/** The RGMATableDefinition structure holds the column definitions for a table.
 *
 *  It is allocated on the heap and you should call RGMA_freeTableDefinition to
 *  free it when it is no longer required.
 */

typedef struct {
    /** Table name. */
    char *tableName;
    /** viewFor (if a view) */
    char *viewFor;
    /** Number of columns in the table. */
    int numColumns;
    /** List of column definitions. */
    RGMAColumnDefinition *column;
} RGMATableDefinition;

/* Primary Producer Functions */

/**
 *  Creates a new Primary Producer resource.
 *
 *
 *  @param  storageType         The tuple storage type for the new producer.
 *                              One of:                             \n
 *                                  RGMAStorageType_MEMORY,         \n
 *                                  RGMAStorageType_DATABASE.
 *  @param  logicalName         Logical name for permanent database tuple store.
 *                              This may be NULL to denote temporary storage.
 *  @param  supportedQueries    One of:                             \n
 *                                  RGMASupportedQueries_C          \n
 *                                  RGMASupportedQueries_CH         \n
 *                                  RGMASupportedQueries_CL         \n
 *                                  RGMASupportedQueries_CHL        \n
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *  @return pointer to primary producer resource.
 */
extern RGMAPrimaryProducer *RGMAPrimaryProducer_create(RGMAStorageType storageType, char *logicalName,
        RGMASupportedQueries supportedQueries, RGMAException **exceptionPP);

/**
 *  Closes a  Primary Producer. This call also frees the memory held by the primary producer.
 *
 *  @param  resourceP             Pointer to the Primary Producer
 *  @param  exceptionPP           Pointer to pointer to an RGMAException.
 */
extern void RGMAPrimaryProducer_close(RGMAPrimaryProducer *resourceP, RGMAException **exceptionPP);

/**
 *  Registers a Primary Producer in the Registry, as a producer for the
 *  specified table.
 *
 *  @param  resourceP              Pointer to the Primary Producer
 *  @param  tableName          The name of the table to declare.
 *  @param  predicate              An SQL WHERE clause defining the subset of
 *                                 the table that this producer will publish or an empty string.
 *  @param  hrpSec                 Retention period (in seconds).
 *                                 Tuples older than this will be removed from
 *                                 the history tuple storage.
 *  @param  lrpSec                 Retention period (in seconds).
 *                                 Tuples older than this will never be returned
 *                                 in latest queries and will be removed from
 *                                 the latest tuple storage.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 */
extern void RGMAPrimaryProducer_declareTable(RGMAPrimaryProducer *resourceP, const char *tableName,
        const char *predicate, int hrpSec, int lrpSec, RGMAException **exceptionPP);

/**
 *  Destroy a  Primary Producer. This call also frees the memory held by the primary producer.
 *
 *  @param  resourceP             Pointer to the Primary Producer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 */
extern void RGMAPrimaryProducer_destroy(RGMAPrimaryProducer *resourceP, RGMAException **exceptionPP);

/**
 * Return the resource endpoint to be used in the constructor of a consumer to make a "directed query".
 *
 *  @param  resourceP           Pointer to the resource.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 * @return the resource endpoint
 */
extern RGMAResourceEndpoint * RGMAPrimaryProducer_getResourceEndpoint(RGMAPrimaryProducer *resourceP,
        RGMAException **exceptionPP);

/**
 *  Publishes data for a single tuple (row) into a table, using an SQL INSERT
 *  statement.
 *
 *  @param  resourceP              Pointer to the Primary Producer resource
 *                                 through which to publish data.
 *  @param  insertStatement    An SQL INSERT statement.
 *  @param  lrpSec                 Retention period (in seconds). Set this to
 *                                 0 to use the default set in
 *                                 RGMAPrimaryProducer_declareTable().
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 */
extern void RGMAPrimaryProducer_insert(RGMAPrimaryProducer *resourceP, const char *insertStatement, int lrpSec,
        RGMAException **exceptionPP);

/**
 *  Publishes data for a single tuple (row) into a table, using an SQL INSERT
 *  statement.
 *
 *  @param  resourceP              Pointer to the Primary Producer resource
 *                                 through which to publish data.
 *  @param  insertStatements       An array of SQL INSERT statements.
 *  @param numInserts              Number of INSERT statements.
 *  @param  lrpSec                 Retention period (in seconds). Set this to
 *                                 0 to use the default set in
 *                                 RGMAPrimaryProducer_declareTable().
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 */
extern void RGMAPrimaryProducer_insertList(RGMAPrimaryProducer *resourceP, int numInserts, char **insertStatements,
        int lrpSec, RGMAException **exceptionPP);

/* Secondary Producer Functions */

/**
 *  Creates a new Secondary Producer.
 *
 *  @param  storageType         The tuple storage type for the new producer.
 *                              One of:                             \n
 *                                  RGMAStorageType_MEMORY,         \n
 *                                  RGMAStorageType_DATABASE.
 *  @param  logicalName         Logical name for permanent database tuple store.
 *                              This may be NULL to denote temporary storage.
 *  @param  supportedQueries    One of:                             \n
 *                                  RGMASupportedQueries_C          \n
 *                                  RGMASupportedQueries_CH         \n
 *                                  RGMASupportedQueries_CL         \n
 *                                  RGMASupportedQueries_CHL        \n
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *  @return pointer to primary producer resource.
 */
extern RGMASecondaryProducer *RGMASecondaryProducer_create(RGMAStorageType storageType, char *logicalName,
        RGMASupportedQueries supportedQueries, RGMAException **exceptionPP);

/**
 *  Closes a  Secondary Producer. This call also frees the memory held by the secondary producer.
 *
 *  @param  resourceP             Pointer to the Secondary Producer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 */
extern void RGMASecondaryProducer_close(RGMASecondaryProducer *resourceP, RGMAException **exceptionPP);

/**
 *  Registers a Secondary Producer in the Registry, as a producer for the
 *  specified table.
 *
 *  @param  resourceP              Pointer to the Secondary Producer.
 *  @param  tableName              The name of the table to declare.
 *  @param  predicate              An SQL WHERE clause defining the subset of
 *                                 the table that this producer will publish.
 *                                 May an empty string.
 *  @param  hrpSec                 Retention period (in seconds).
 *                                 Tuples older than this will be removed from
 *                                 the history tuple storage.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 */
extern void RGMASecondaryProducer_declareTable(RGMASecondaryProducer *resourceP, const char *tableName,
        const char *predicate, int hrpSec, RGMAException **exceptionPP);

/**
 *  Destroy a  Secondary Producer.
 *
 *  @param  resourceP             Pointer to the Secondary Producer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 */
extern void RGMASecondaryProducer_destroy(RGMASecondaryProducer *resourceP, RGMAException **exceptionPP);

/**
 * Return the resource endpoint to be used in the constructor of a consumer to make a "directed query".
 *
 *  @param  resourceP           Pointer to the resource.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 * @return the resource endpoint
 */
extern RGMAResourceEndpoint * RGMASecondaryProducer_getResourceEndpoint(RGMASecondaryProducer *resourceP,
        RGMAException **exceptionPP);

/**
 * Return the resouceId to be used as the argument to subsequent RGMASecondaryProducer_staticShowSignOfLife() calls.
 *
 *  @param  resourceP             Pointer to the Secondary Producer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 * @return the resourceId
 */
extern int RGMASecondaryProducer_getResourceId(RGMASecondaryProducer *resourceP, RGMAException **exceptionPP);

/**
 *  Indicates to a service that a resource should be kept alive. This
 *  method should be called periodically if there is no other contact with the
 *  secondary producer through another API function.
 *
 *  @param  resourceP           Pointer to the resource to keep alive.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 */
extern void RGMASecondaryProducer_showSignOfLife(RGMASecondaryProducer *resourceP, RGMAException ** exceptionPP);

/**
 *  Indicates to a service that a resource should be kept alive. This
 *  method should be called periodically if there is no other contact with the
 *  resource through another API function.
 *
 *  @param  resourceId           Id of resource to keep alive.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return 1 if it is alive or 0 if not.
 */
extern int RGMASecondaryProducer_staticShowSignOfLife(int resourceId, RGMAException ** exceptionPP);

/* OnDemand Producer Functions */

/**
 *  Creates a new OnDemand Producer resource.
 *
 *  @param hostName             the host name of the system that will respond to queries
 *  @param port             the port on the specified host that will respond to queries
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *  @return pointer to OnDemand producer.
 */
extern RGMAOnDemandProducer *RGMAOnDemandProducer_create(const char *hostName, int port, RGMAException **exceptionPP);

/**
 *  Closes an OnDemand Producer.
 *
 *  @param  resourceP              Pointer to the OnDemand Producer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 */
extern void RGMAOnDemandProducer_close(RGMAOnDemandProducer *resourceP, RGMAException **exceptionPP);

/**
 *  Registers an OnDemand Producer in the Registry, as a producer for the
 *  specified table.
 *
 *  @param  resourceP             Pointer to the OnDemand producer
 *  @param  tableName          The name of the table to declare
 *  @param  predicate              An SQL WHERE clause defining the subset of
 *                                 the table that this producer will publish or an empty string.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return 0 on success or -1 on error.
 */
extern void RGMAOnDemandProducer_declareTable(RGMAOnDemandProducer *resourceP, const char *tableName,
        const char *predicate, RGMAException **exceptionPP);

/**
 *  Destroy an OnDemand Producer.
 *
 *  @param  resourceP             Pointer to the OnDemand producer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 */
extern void RGMAOnDemandProducer_destroy(RGMAOnDemandProducer *resourceP, RGMAException **exceptionPP);

/**
 * Return the resource endpoint to be used in the constructor of a consumer to make a "directed query".
 *
 *  @param  resourceP            Pointer to the OnDemand producer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 * @return the resource endpoint
 */
extern RGMAResourceEndpoint * RGMAOnDemandProducer_getResourceEndpoint(RGMAOnDemandProducer *resourceP,
        RGMAException **exceptionPP);

/* Consumer functions */

/**
 * Creates a consumer with the specified query and query type. If a query interval is specified
 * the query will return queries up to some time in the past.
 * If a timeout is specified the query will terminate after that time interval. If the query was still running it will
 * have ``aborted`` status. Normally the mediator is used to select producers however a list of producers may
 * also be specified explicitly.
 *
 * @param query: a SQL select statement
 *
 * @param queryType: the type of the query. This must be one of the constants RGMAQueryType_C,
 * RGMAQueryType_H, RGMAQueryType_L
 * or RGMAQueryType_S
 * unless a queryInterval is specified (i.e. non zero) in which case it must be one of  RGMAQueryTypeWithInterval_C,
 * RGMAQueryTypeWithInterval_H or RGMAQueryTypeWithInterval_L.
 *
 * @param queryInterval: the time interval in seconds is subtracted from the current time to give a time in the past
 * from which to consider tuples. If the interval is zero the interval is considered to be unspecified.
 *
 *  @param timeout: if non zero the time interval after which the query will be aborted.
 *
 *  @param numProducers Number of producers to use in a directed query. If this is zero the mediator will be used.
 *
 *  @param producers: list of producers to contact for a directed query.
 *
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return pointer to primary producer resource.
 *
 */
extern RGMAConsumer * RGMAConsumer_create(const char* query, RGMAQueryType queryType, int queryInterval, int timeout,
        int numProducers, RGMAResourceEndpoint * producers, RGMAException ** exceptionPP);

/**
 *  Aborts a running query, after a call to start() or startDirected()). Does
 *  nothing if the query has already finished. Any tuples which had already
 *  been delivered to the consumer service may still be retrieved via pop.
 *
 *  @param  resourceP              Pointer to the Consumer resource which
 *                                 started the query.
 *
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return 0 on success or -1 on error.
 */
extern void RGMAConsumer_abort(RGMAConsumer *resourceP, RGMAException ** exceptionPP);

/**
 *  Closes a  Consumer.
 *
 *  @param  resourceP              Pointer to the Consumer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 */
extern void RGMAConsumer_close(RGMAConsumer *resourceP, RGMAException **exceptionPP);

/**
 *  Destroy a  Consumer.
 *
 *  @param  resourceP              Pointer to the Consumer
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 */
extern void RGMAConsumer_destroy(RGMAConsumer *resourceP, RGMAException **exceptionPP);

/**
 *  Determines whether a Consumer's query has aborted (either following a call
 *  to abort(), or because its query has timed out).
 *
 *  Note that as this involves a remote call, you should be careful to check
 *  for errors.
 *
 *  @param  resourceP              Pointer to the Consumer resource to query.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return 1 if aborted else 0.
 *
 */
extern int RGMAConsumer_hasAborted(RGMAConsumer *resourceP, RGMAException ** exceptionPP);

/**
 *  Retrieves at most maxCount tuples from a Consumer which have been returned
 *  by producers. The returned list will be empty if there are no tuples
 *  currently available. The final result set for a query will have its
 *  endOfResults flag set to 1 (true).
 *
 *  You should free the result set when no longer required, by calling
 *  RGMA_freeTupleSet().
 *
 *  @param  resourceP              Pointer to the Consumer resource to query.
 *  @param  maxCount               The maximum number of tuples to retrieve.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return Pointer to new result set, or NULL on error.
 *  @see RGMATupleSet, RGMA_printTupleSet(), RGMA_freeTupleSet().
 */
extern RGMATupleSet *RGMAConsumer_pop(RGMAConsumer *resourceP, int maxCount, RGMAException ** exceptionPP);

/* Registry functions */

/**
 *  Returns details of all producers that are registered for a specified table
 *  in a specified virtual database.
 *
 *  @param  vdbName           Virtual database name.
 *  @param  tableName         Table name.
 *  @param  exceptionPP        Pointer to exception (filled in on failure).
 *
 *  @return List of producers (may be empty) or NULL on error.
 */
extern RGMAProducerTableEntryList *RGMARegistry_getAllProducersForTable(const char *vdbName, const char *tableName,
        RGMAException **exceptionPP);

/* Schema functions */

/**
 *  Creates a new table in the schema of a specified virtual database. The
 *  table name, column names and column types must conform to the rules in the
 *  User Guide. A set of initial authorization rules for the table may also
 *  be specified. The format of the rules is described in the User Guide.
 *
 *  @param  vdbName               Virtual database name.
 *  @param  createTableStatement  SQL CREATE TABLE statement.
 *  @param  numRules              Number of authorization rules (may be 0).
 *  @param  rules                 Authorization rules (may be NULL).
 *  @param  exceptionPP            Pointer to exception (filled in on failure).
 *
 *  @return 0 on success or -1 on error.
 */
extern int RGMASchema_createTable(const char *vdbName, const char *createTableStatement, int numRules, char **rules,
        RGMAException **exceptionPP);

/**
 *  Drops a table from the schema of a specified virtual database.
 *
 *  @param  vdbName              Virtual database name.
 *  @param  tableName            Table to drop.
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return 0 on success or -1 on error.
 */
extern int RGMASchema_dropTable(const char *vdbName, const char *tableName, RGMAException **exceptionPP);

/**
 *  Creates a new index definition for a table in the schema of a specified
 *  virtual database. The index name and index definition must conform to the
 *  rules in the User Guide.
 *
 *  @param  vdbName               Virtual database name.
 *  @param  createIndexStatement  SQL CREATE INDEX statement.
 *  @param  exceptionPP            Pointer to exception (filled in on failure).
 *
 *  @return 0 on success or -1 on error.
 */
extern int RGMASchema_createIndex(const char *vdbName, const char *createIndexStatement, RGMAException **exceptionPP);

/**
 *  Drops an index from the schema of a specified virtual database.
 *
 *  @param  vdbName              Virtual database name.
 *  @param  tableName            Table with index to drop.
 *  @param  indexName            Index to drop.
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return 0 on success or -1 on error.
 */
extern int RGMASchema_dropIndex(const char *vdbName, const char *tableName, const char *indexName,
        RGMAException **exceptionPP);

/**
 *  Creates a new view in the schema of a specified virtual database. The
 *  view name and view definition must conform to the rules in the User Guide.
 *  A set of initial authorization rules for the view may also be specified.
 *  The format of the rules is described in the User Guide.
 *
 *  @param  vdbName               Virtual database name.
 *  @param  createViewStatement   SQL CREATE VIEW statement.
 *  @param  numRules              Number of authorization rules (may be 0).
 *  @param  rules                 Authorization rules (may be NULL).
 *  @param  exceptionPP            Pointer to exception (filled in on failure).
 *
 *  @return 0 on success or -1 on error.
 */
extern int RGMASchema_createView(const char *vdbName, const char *createViewStatement, int numRules, char **rules,
        RGMAException **exceptionPP);

/**
 *  Drops a view from the schema of a specified virtual database.
 *
 *  @param  vdbName              Virtual database name.
 *  @param  viewName             View to drop.
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return 0 on success or -1 on error.
 */
extern int RGMASchema_dropView(const char *vdbName, const char *viewName, RGMAException **exceptionPP);

/**
 *  Returns a list of all tables and views in the schema of a specified
 *  virtual database.
 *
 *  You should free the list returned by calling RGMA_freeStringList when
 *  it is no longer required.
 *
 *  @param  vdbName              Virtual database name.
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return List of tables and views (may be empty) or NULL on error.
 */
extern RGMAStringList *RGMASchema_getAllTables(const char *vdbName, RGMAException **exceptionPP);

/**
 *  Returns the column definitions of a table or view in the schema of a
 *  specified virtual database.
 *
 *  You should free the structure returned by calling RGMA_freeTableDefinition
 *  when it is no longer required.
 *
 *  @param  vdbName              Virtual database name.
 *  @param  tableName            Table or view to query.
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return Table definition or NULL on error.
 */
extern RGMATableDefinition *RGMASchema_getTableDefinition(const char *vdbName, const char *tableName,
        RGMAException **exceptionPP);

/**
 *  Returns the list of indexes defined on a table in the schema of a
 *  specified virtual database.
 *
 *  You should free the list returned by calling RGMA_freeIndexList when
 *  it is no longer required.
 *
 *  @param  vdbName              Virtual database name.
 *  @param  tableName            Table to query.
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return List of indexes (may be empty) or NULL on error.
 */
extern RGMAIndexList *RGMASchema_getTableIndexes(const char *vdbName, const char *tableName,
        RGMAException **exceptionPP);

/**
 *  Replaces the authorization rules on a table or view in the schema of a
 *  specified virtual database. The format of the rules is described in the
 *  User Guide.
 *
 *  @param  vdbName              Virtual database name.
 *  @param  tableName            Table or view to update.
 *  @param  numRules             Number of authorization rules (may be 0 to
 *                               remove all existing rules).
 *  @param  rules                Authorization rules (may be NULL).
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return 0 on success or -1 on error.
 */

extern int RGMASchema_setAuthorizationRules(const char *vdbName, const char *tableName, int numRules, char **rules,
        RGMAException **exceptionPP);

/**
 *  Returns the authorization rules on a table or view in the schema of a
 *  specified virtual database. The format of the rules is described in the
 *  User Guide.
 *
 *  You should free the list returned by calling RGMA_freeStringList when
 *  it is no longer required.
 *
 *  @param  vdbName              Virtual database name.
 *  @param  tableName            Table or view to update.
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return List of authorization rules (may be empty) or NULL on error.
 */
extern RGMAStringList *RGMASchema_getAuthorizationRules(const char *vdbName, const char *tableName,
        RGMAException **exceptionPP);

/* Service functions */

/**
 *  Returns a list of names and database parameters of all permanent tuple
 *  stores belonging to the user (or all tuple stores in the service if it
 *  is running insecurely).
 *
 *  You should free the list when no longer required, by calling
 *  RGMA_freeTupleStoreList().
 *
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return RGMATupleStoreList containing zero or more tuple store definitions,
 *          or NULL on error.
 */
extern RGMATupleStoreList *RGMA_listTupleStores(RGMAException **exceptionPP);

/**
 *  Drops one of user's permanent tuple stores. Tuple stores that are currently
 *  being used by a producer cannot be dropped.
 *
 *  @param  logicalName          Logical name of tuple store to be dropped.
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return 0 on success or -1 on error.
 */
extern void RGMA_dropTupleStore(const char *logicalName, RGMAException **exceptionPP);

/**
 *  Queries the termination interval for a resource.
 *
 *  @param  exceptionPP            Pointer to pointer to an RGMAException.
 *
 *  @return Termination interval (in seconds).
 */
extern int RGMA_getTerminationInterval(RGMAException **exceptionPP);

/**
 *  Returns a string representing the version number of an R-GMA service.
 *
 *  You should free the string returned (by calling free()) when it is no
 *  longer required.
 *
 *  @param  exceptionPP           Pointer to exception (filled in on failure).
 *
 *  @return Version number string or NULL on error.
 */
extern char *RGMA_getVersion(RGMAException **exceptionPP);

/* Functions to free memory */

/**
 *  Frees all memory associated with an RGMAResourceEndpoint structure.
 *
 *  @param  endpointP           Pointer to the RGMAResourceEndpoint to free.
 *
 *  @return Nothing.
 *  @see RGMAResourceEndpoint
 */
extern void RGMA_freeResourceEndpoint(RGMAResourceEndpoint *endpointP);

/**
 *  Frees all memory contained within an RGMAException structure (but does not
 *  free the exception structure itself).
 *
 *  @param  exceptionP           Pointer to the RGMAException to free.
 *
 *  @return Nothing.
 *  @see RGMAException
 */
extern void RGMA_freeException(RGMAException *exceptionP);

/**
 *  Frees all memory associated with an RGMAStringList structure.
 *
 *  @param  stringListP       Pointer to the RGMAStringList to free.
 *
 *  @return Nothing.
 *  @see    RGMAStringList
 */
extern void RGMA_freeStringList(RGMAStringList *stringListP);

/**
 *  Frees all memory associated with an RGMATupleSet structure.
 *
 *  @param  tupleSetP           Pointer to the RGMATupleSet to free.
 *
 *  @return Nothing.
 *  @see    RGMATupleSet
 */
extern void RGMA_freeTupleSet(RGMATupleSet *tupleSetP);

/**
 *  Frees all memory associated with an RGMATupleStoreList structure.
 *
 *  @param  tupleStoreListP       Pointer to the RGMATupleStoreList to free.
 *
 *  @return Nothing.
 *  @see    RGMATupleStoreList
 */
extern void RGMA_freeTupleStoreList(RGMATupleStoreList *tupleStoreListP);

/**
 *  Frees all memory associated with an RGMAProducerTableEntryList structure.
 *
 *  @param  producerTableEntryListP    Pointer to the RGMAProducerTableEntryList
 *                                     to free.
 *
 *  @return Nothing.
 *  @see    RGMAProducerTableEntryList
 */
extern void RGMA_freeProducerTableEntryList(RGMAProducerTableEntryList *producerTableEntryListP);

/**
 *  Frees all memory associated with an RGMAIndexList structure.
 *
 *  @param  indexListP       Pointer to the RGMAIndexList to free.
 *
 *  @return Nothing.
 *  @see    RGMAIndexList
 */
extern void RGMA_freeIndexList(RGMAIndexList *indexListP);

/**
 *  Frees all memory associated with an RGMATableDefinition structure.
 *
 *  @param  tableDefinitionP       Pointer to the RGMATableDefinition to free.
 *
 *  @return Nothing.
 *  @see    RGMATableDefinition
 */
extern void RGMA_freeTableDefinition(RGMATableDefinition *tableDefinitionP);

/******************************************************************************/

#ifdef __cplusplus
}
#endif

#endif /* RGMA_H */

/* End of file. */
