/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;


/**
 * A column name and value pair.
 */
public class ColumnValue {
    /** Column name. */
    private String m_name;

    /** Column value. */
    private Constant m_value;

    /**
     * Creates a new ColumnValue with the given name and value.
     *
     * @param name Column name.
     * @param value Column value.
     */
    public ColumnValue(String name, Constant value) {
        m_name = name;
        m_value = value;
    }

    /**
     * Gets the column name.
     *
     * @return The column name.
     */
    public String getName() {
        return m_name;
    }

    /**
     * Gets the column value.
     *
     * @return The column value.
     */
    public Constant getValue() {
        return m_value;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        return m_name + " = " + m_value;
    }
}
