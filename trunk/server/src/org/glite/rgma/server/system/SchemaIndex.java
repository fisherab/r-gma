/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

import java.util.List;


/**
 * An index on a table.
 */
public class SchemaIndex {
    private final String m_name;
    private final List<String> m_columnNames;

    /**
     * Constructor.
     *
     * @param name Index name
     * @param columnNames Columns to index on.
     */
    public SchemaIndex(String name, List<String> columnNames) {
        m_name = name;
        m_columnNames = columnNames;
    }

    /**
     * Returns the List of column names used by this index.
     *
     * @return List of column names.
     */
    public List<String> getColumnNames() {
        return m_columnNames;
    }

    /**
     * Returns the name of the given column used by this index.
     *
     * @param columnNumber Number of the column (0..numColumns-1)
     *
     * @return Name associated with the given column number.
     */
    public String getColumnName(int columnNumber) {
        return (String) m_columnNames.get(columnNumber);
    }

    /**
     * Gets the name of this index.
     *
     * @return This index name.
     */
    public String getName() {
        return m_name;
    }

    public String toString() {
        return "Index[" + m_name + ", columns=" + m_columnNames + "]";
    }
}
