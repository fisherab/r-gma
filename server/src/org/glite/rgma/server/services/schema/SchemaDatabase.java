/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.schema;

import java.util.List;

import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.SchemaIndex;
import org.glite.rgma.server.system.SchemaTableDefinition;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UserContextInterface;

/**
 * Schema database interface for various underlying DB implementations.
 */
interface SchemaDatabase {
	boolean alter(String torv, String tableName, String action, String name, String type, UserContextInterface requestContext) throws RGMAPermanentException;

	boolean createIndex(String createIndexStatement, UserContextInterface requestContext) throws RGMAPermanentException;

	boolean createTable(String crateTableStatement, List<String> tableAuthz, UserContextInterface requestContext) throws RGMAPermanentException;

	boolean createView(String crateViewStatement, List<String> viewAuthz, UserContextInterface requestContext) throws RGMAPermanentException;

	boolean dropIndex(String tableName, String indexName, UserContextInterface requestContext) throws RGMAPermanentException;

	boolean dropTable(String tableName, UserContextInterface requestContext) throws RGMAPermanentException;

	boolean dropView(String viewName, UserContextInterface requestContext) throws RGMAPermanentException;

	List<TupleSet> getAllSchema() throws RGMAPermanentException;

	List<String> getAllTables(UserContextInterface userContext) throws RGMAPermanentException;

	List<String> getAuthorizationRules(String tableName, UserContextInterface requestContext) throws RGMAPermanentException;

	long getMasterTime() throws RGMAPermanentException;

	List<TupleSet> getSchemaUpdates(long timeStamp) throws RGMAPermanentException;

	SchemaTableDefinition getTableDefinition(String tableName, UserContextInterface requestContext) throws RGMAPermanentException;

	List<SchemaIndex> getTableIndexes(String tableName, UserContextInterface requestContext) throws RGMAPermanentException;

	long getTableTimestamp(String tableName) throws RGMAPermanentException;

	void putAllSchema(List<TupleSet> fullSchema) throws RGMAPermanentException;

	void putSchemaUpdates(List<TupleSet> fullSchema) throws RGMAPermanentException;

	boolean setAuthorizationRules(String tableName, List<String> authzRules, UserContextInterface requestContext) throws RGMAPermanentException;

	void setSchemaRules(List<String> rules);

	void shutdown();

	boolean tableExists(String tableName) throws RGMAPermanentException;

	boolean viewExists(String viewName) throws RGMAPermanentException;
}
