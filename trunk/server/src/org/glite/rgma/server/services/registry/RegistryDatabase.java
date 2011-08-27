/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.registry;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.glite.rgma.server.system.ConsumerEntry;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;

interface RegistryDatabase {

	void addRegistration(RegistryConsumerResource consumer, boolean isMaster) throws RGMAPermanentException;

	/**
	 * Registers a resource in the RegistryDatabase
	 * 
	 * @param resource
	 *            the resource to be registered
	 * @throws SQLException
	 * @throws RegistryException
	 *             if resource is already registered
	 */
	void addRegistration(RegistryProducerResource resource, boolean isMaster) throws RGMAPermanentException;

	/**
	 * Flags a registration as deleted
	 * 
	 * @param entry
	 *            the entry representing the resource to be deleted
	 * @throws SQLException
	 * @throws RegistryException
	 */
	void deleteRegistration(UnregisterConsumerResourceRequest consumer) throws RGMAPermanentException;

	void deleteRegistration(UnregisterProducerTableRequest producer) throws RGMAPermanentException;

	/**
	 * Get all of the producers currently registered for the table specified
	 * 
	 * @param tableName
	 *            the name of the table that the producer search is done on
	 * @return A List of Producer table entries currently registered for the
	 *         table
	 * @throws SQLException
	 * @throws RegistryException
	 */
	List<ProducerTableEntry> getAllProducersForTable(String tableName) throws RGMAPermanentException;

	/**
	 * Gets all consumers (of the required table(s)/type(s)) that don't
	 * contradict the predicate
	 * 
	 * @param producer
	 *            the producer to match against the registered consumers
	 * @return ConsumerEntryList containing all consumers (of the required
	 *         table/type) that don't contradict the predicate
	 * @throws SQLException
	 * @throws RegistryException
	 */
	List<ConsumerEntry> getConsumersMatchingPredicate(RegistryProducerResource producer) throws RGMAPermanentException;

	String getConsumerTableEntriesForHostName(String hostName) throws RGMAPermanentException;

	String getConsumerTableEntriesForTableName(String tableName) throws RGMAPermanentException;

	/**
	 * get all of the records that would constitute a replca of producers and
	 * continuous consumers in this vdb that this vdb is master of
	 * @param now 
	 * 
	 * @return a hashmap of result sets that contains all of the producers and
	 *         continuous consumers and fixed columns in this vdb
	 * @throws DatabaseException
	 * @throws RGMAPermanentException
	 * @throws SQLException
	 */
	ReplicationMessage getFullUpdateRecords(int now) throws RGMAPermanentException;

	int getLastReplicationTimeSecs(String hostname) throws RGMAPermanentException;

	/**
	 * Gets all producers (of the required table(s)/type(s)) that don't
	 * contradict the predicate
	 * 
	 * @param select
	 *            the select statement from the consumer
	 * @return ProducerTableEntryList containing all producers (of the required
	 *         table/type) that don't contradict the predicate
	 * @throws SQLException
	 * @throws RegistryException
	 */
	List<ProducerTableEntry> getProducersMatchingPredicate(List<String> tableNames, String predicate,
			QueryProperties queryProperties, boolean isSecondary)
			throws RGMAPermanentException;

	String getProducerTableEntriesForHostName(String hostName) throws RGMAPermanentException;

	String getProducerTableEntriesForTableName(String tableName) throws RGMAPermanentException;

	int getRegisteredConsumerCount(boolean masterOnly) throws RGMAPermanentException;

	int getRegisteredProducerCount(boolean masterOnly) throws RGMAPermanentException;

	List<String> getUniqueHostNames() throws RGMAPermanentException;

	List<String> getUniqueTableNames() throws RGMAPermanentException;

	/**
	 * Deletes expired registrations from the database
	 * 
	 * @throws SQLException
	 */
	void purgeExpiredRegistrations() throws RGMAPermanentException;

	void setLastReplicationTime(String hostname, int time) throws RGMAPermanentException;

	/**
	 * shutdown this database
	 * 
	 * @throws RGMAException
	 */
	void shutdown();

	Set<UnregisterProducerTableRequest> getProducersFromRemote(String hostName) throws RGMAPermanentException;

	Set<UnregisterConsumerResourceRequest> getConsumersFromRemote(String hostName)throws RGMAPermanentException;

}
