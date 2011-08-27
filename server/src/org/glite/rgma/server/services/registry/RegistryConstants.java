package org.glite.rgma.server.services.registry;

public class RegistryConstants {

	protected RegistryConstants() {
	}

	/** Logger for registry operations */
	public static final String REGISTRY_LOGGER = "rgma.services.registry";

	/**  Logger for the registry database 	 */
	public static final String REGISTRY_DATABASE_LOGGER = "rgma.services.registry.database";

	/** Logger for registry replication operations */
	public static final String REGISTRY_REPLICATION_LOGGER = "rgma.services.registry.replication";
	
	/** Logger for registry cleanup operations */
	public static final String REGISTRY_CLEANUP_LOGGER = "rgma.services.registry.cleanup";


	/** Column names */
	public static final String TABLE_NAME = "tableName";
	public static final String ID = "ID";
	public static final String URL = "URL";
	public static final String PREDICATE = "predicate";
	public static final String TI_SECS = "TISECS";
	public static final String IS_CONTINUOUS = "isContinuous";
	public static final String IS_HISTORY = "isHistory";
	public static final String IS_LATEST = "isLatest";
	public static final String IS_STATIC = "isStatic";
	public static final String IS_SECONDARY = "isSecondary";
	public static final String HRP_SECS = "HRPSecs";

	/** Identifier for the Task ownerid */
	public static final String REGISTRY_REPLICATION_THREAD = "RegistryReplicationThread";

	/** table names for the fixed colums */
	public static final String FIXED_INT_COLUMNS = "fixedIntColumns";
	public static final String FIXED_REAL_COLUMNS = "fixedRealColumns";
	public static final String FIXED_STRING_COLUMNS = "fixedStringColumns";

}
