/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;


/**
 * Storage location for tuples:
 *
 * <ul>
 * <li>
 * MEMORY: <code>Storage.MEMORY</code>
 * </li>
 * <li>
 * default DATABASE: <code>Storage.DATABASE</code>
 * </li>
 * <li>
 * specific DATABASE: <code>Storage.getDatabase(logicalName)</code>
 * </li>
 * </ul>
 */
public class Storage {
    /** Logical name of storage location. */
    private String m_logicalName;

    /** Type of storage. */
    private StorageType m_type;
    
    /** Database storage, no logical name. */
    public static final Storage DATABASE = new Storage(StorageType.DB);
    
    /** Memory storage, no logical name. */
    public static final Storage MEMORY = new Storage(StorageType.MEM);

    /**
     * Creates a new Storage object.
     *
     * @param type Type.MEM or Type.DB.
     */
    private Storage(StorageType type) {
        m_type = type;
        m_logicalName = "";
    }

    /**
     * Creates a new Storage object.
     *
     * @param type Type of storage.
     * @param logicalName Logical name of storage medium.
     */
    private Storage(StorageType type, String logicalName) {
        m_type = type;
        m_logicalName = logicalName;
    }
    
    /**
     * Gets a DATABASE Storage object for the specified database.
     *
     * @param logicalName The logical name of the storage system
     *
     * @return A Storage object for the specified database.
     */
    public static Storage getDatabase(String logicalName) {
        return new Storage(StorageType.DB, logicalName);
    }
    
    /**
     * Determines if this storage system is a database.
     *
     * @return <code>true</code> if the storage system is a database.
     */
    public boolean isDatabase() {
        return (m_type == StorageType.DB);
    }

    /**
     * Determines if this storage system is memory.
     *
     * @return <code>true</code> if the storage system is memory.
     */
    public boolean isMemory() {
        return (m_type == StorageType.MEM);
    }

    /**
     * Gets the logical name for this storage system.
     *
     * @return Logical name as a String
     */
    public String getLogicalName() {
        return m_logicalName;
    }

//    /**
//     * Compares this Storage to the specified location.
//     *
//     * @param obj Another Storage object.
//     *
//     * @return <code>true</code> if the specified location is identical to this location.
//     */
//    public boolean equals(Object obj) {
//        if (!(obj instanceof Storage)) {
//            return false;
//        }
//
//        Storage storage = (Storage) obj;
//
//        return (m_type == storage.m_type) && (m_logicalName != null) && (storage.m_logicalName != null) && storage.equals(m_logicalName);
//    }
   
    public String toString() {
    	return m_type + ":" + m_logicalName;
    }

//    /**
//     * Returns a hash code value for this Storage object.
//     *
//     * @return Hash code value for this Storage object.
//     */
//    public int hashCode() {
//        return (m_type + m_logicalName).hashCode();
//    }

    /**
     * Gets the location type of this Storage.
     *
     * @return Either Type.MEM or Type.DB.
     */
    protected StorageType getType() {
        return m_type;
    }

    /** Storage type. */
    public enum StorageType {MEM, 
        DB;
    }
}
