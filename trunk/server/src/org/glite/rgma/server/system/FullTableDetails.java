/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

import java.util.List;

/**
 * Contains the table definition and authorization rules for a
 * table in the schema.
 */
public class FullTableDetails {
    /** The table definition. */
    private final SchemaTableDefinition m_tableDefinition;

    /** Authorization rules of the table. */
    private final TableAuthorization m_tableAuthorization;

    /** Columns that form the view. */
    private final String[] m_viewColumns;

    /** Indexes on the table. */
    private final List<SchemaIndex> m_indexes;

    /**
     * Constructor for a view.
     *
     * @param tableDefinition Table definition.
     * @param tableAuthorization Table read/write authorisation rules.
     * @param viewColumns Columns which form the view.
     * @param indexes List of column indexes.
     */
    public FullTableDetails(SchemaTableDefinition tableDefinition, TableAuthorization tableAuthorization,
                            String[] viewColumns, List<SchemaIndex> indexes) {
        m_tableDefinition = tableDefinition;
        m_tableAuthorization = tableAuthorization;
        m_viewColumns = viewColumns;
        m_indexes = indexes;
    }

    /**
     * Constructor for a table.
     *
     * @param tableDefinition Table definition.
     * @param tableAuthorization Table read/write authorisation rules.
     * @param indexes List of column indexes.
     */
    public FullTableDetails(SchemaTableDefinition tableDefinition, TableAuthorization tableAuthorization,
                            List<SchemaIndex> indexes) {
        m_tableDefinition = tableDefinition;
        m_tableAuthorization = tableAuthorization;
        m_viewColumns = null;
        m_indexes = indexes;
    }

    /**
     * @return Table authorization rules.
     */
    public TableAuthorization getTableAuthorization() {
        return m_tableAuthorization;
    }

    /**
     * @return Table definition.
     */
    public SchemaTableDefinition getTableDefinition() {
        return m_tableDefinition;
    }

    /**
     * @return List of indexes on the table.
     */
    public List<SchemaIndex> getIndexes() {
        return m_indexes;
    }

    /**
     * @return List of column names which form the view.
     */
    public String[] getViewColumns() {
        return m_viewColumns;
    }

    /**
     * @return <code>true</code> if the table has view columns..
     */
    public boolean hasViewColumns() {
        return m_viewColumns != null;
    }
}
