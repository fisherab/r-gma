/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * Defines all SQL data types supported by R-GMA.
 */
public class DataType {
    /** Data type. */
    private Type m_type;

    /** Size/precision of data. */
    private int m_size;

    /**
     * Creates a new DataType.
     */
    public DataType() {
    }

    /**
     * Creates a new DataType with the given type and size.
     *
     * @param type Data type.
     * @param size Size/precision (can be zero).
     */
    public DataType(Type type, int size) {
        m_type = type;
        m_size = size;
    }

    /**
     * Gets the size/precision.
     *
     * @return The size/precision.
     */
    public int getSize() {
        return m_size;
    }

    /**
     * Sets the size/precision.
     *
     * @param size The size/precision.
     */
    public void setSize(int size) {
        m_size = size;
    }

    /**
     * Gets the type.
     *
     * @return The type.
     */
    public Type getType() {
        return m_type;
    }

    /**
     * Sets the type.
     *
     * @param type The type.
     */
    public void setType(Type type) {
        m_type = type;
    }

    /**
     * @see Object#equals(java.lang.Object)
     */
    @Override
	public boolean equals(Object obj) {
        if (!(obj instanceof DataType)) {
            return false;
        }
        DataType dt = (DataType) obj;
        return (dt.getType() == m_type) && (dt.getSize() == m_size);
    }

    /**
     * @see Object#hashCode()
     */
    @Override
	public int hashCode() {
        return m_type.hashCode() + m_size;
    }

    /**
     * @see Object#toString()
     */
    @Override
	public String toString() {
        return m_type + ((m_size > 0) ? ("(" + m_size + ")") : "");
    }

    /** Supported types. */
    public enum Type {
        INTEGER (4),
        BIGINT (-5), // Just used internally
        REAL (7),
        DOUBLE_PRECISION (8, "DOUBLE PRECISION"),
        DATE (91),
        TIME (92),
        TIMESTAMP (93),
        CHAR (1),
        VARCHAR (12);

        /** Displayed name to use (can be <code>null</code>). */
        private String m_name;

        /** Code number, used in result sets */
        private int m_code;

        /**
         * Constructs a Type where the displayed name matches the constant
         * name.
         *
         * @param code Code number.
         */
        Type(int code) {
            m_code = code;
        }

        /**
         * Constructs a Type where the displayed name differs from the constant
         * name.
         *
         * @param code Code number.
         * @param name Displayed name to use.
         */
        Type(int code, String name) {
            m_code = code;
            m_name = name;
        }

        /**
         * @see Object#toString()
         */
        @Override
		public String toString() {
            if (m_name == null) {
                return super.toString();
            } else {
                return m_name;
            }
        }

        /**
         * Get the type code number.
         */
        public int getTypeCode() {
            return m_code;
        }

        /**
         * Get a type from its code number.
         */
        public static Type getType(int code) throws RGMAPermanentException {
            for (Type type : Type.values()) {
                if (type.getTypeCode() == code) {
                    return type;
                }
            }

            throw new RGMAPermanentException("No such type with code " + code);
        }

        /**
         * Get a type from its name.
         */
        public static Type getType(String name) throws RGMAPermanentException {
            for (Type type : Type.values()) {
                if (type.toString().equalsIgnoreCase(name)) {
                    return type;
                }
            }

            // Special cases
            if (name.equalsIgnoreCase("DOUBLE_PRECISION") || name.equalsIgnoreCase("DOUBLE")) {
                return DOUBLE_PRECISION;
            }

            throw new RGMAPermanentException("Type '" + name + "' not recognised.");
        }
    }

}
