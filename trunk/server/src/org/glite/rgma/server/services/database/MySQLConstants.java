/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.database;

public class MySQLConstants {
    /** Error code denoting that an entry with the same PK already exists. */
    public static final int ER_DUP_ENTRY = 1062;

    /** MySQL communication error that can be retried. */
    public static final String MYSQL_COMMUNICATION_ERROR_STATE = "08S01";

    /** MySQL deadlock error that can be retried. */
    public static final String MYSQL_DEADLOCK_ERROR_STATE = "40001";

    /** Error code denoting that the table could not be found. */
    public static final int ER_NO_SUCH_TABLE = 1146;

    /** Unknown database error. */
    public static final int ER_BAD_DB_ERROR = 1049;

    /**
     * Don't instantiate constants class.
     */
    private MySQLConstants() {
    }
}
