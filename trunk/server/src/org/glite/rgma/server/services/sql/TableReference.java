/**
 * 
 */
package org.glite.rgma.server.services.sql;

/**
 * A table-reference in SQL92.  Can be a simple (qualified) table name (with alias),
 * a qualified table name using the R-GMA union notation or a join between two table references.
 * 
 * The first two cases use an AliasedName object, the last uses a JoinedTable object.
 * 
 * The parser currently rejects joins so it is safe to assume that an AliasedName is always present
 */
public class TableReference {
    /** Name of table with optional alias. */
    private TableNameAndAlias m_tableNameAndAlias;
    
    /** Details of joined tables. */
    private JoinedTable m_joinedTable;
    
    /** Is the reference a JOIN? */
    private boolean m_join;

    /**
     * Creates a new TableReference object for joined tables.
     * 
     * @param joinedTable Details of joined tables.
     */
    public TableReference(JoinedTable joinedTable) {
        m_joinedTable = joinedTable;
        m_join = true;
    }

    /**
     * Creates a new TableReference object for a simple aliased name.
     * 
     * @param tableName Name of table.
     */
    public TableReference(TableNameAndAlias tableNameAndAlias) {
        m_tableNameAndAlias = tableNameAndAlias;
        m_join = false;
    }
    
    /**
     * Creates a new TableReference object for a simple aliased name.
     * 
     * @param tableName Name of table.
     */
    public TableReference(TableName tableName) {
        m_tableNameAndAlias = new TableNameAndAlias(tableName);
        m_join = false;
    }


    /**
     * Creates a new TableReference with the same attributes as <code>tr</code>.
     * 
     * @param tr TableReference to copy.
     */
    public TableReference(TableReference tr) {
        m_join = tr.m_join;
        if (tr.m_joinedTable != null) {
            m_joinedTable = new JoinedTable(tr.m_joinedTable);
        }
        if (tr.m_tableNameAndAlias != null) {
            m_tableNameAndAlias = new TableNameAndAlias(tr.m_tableNameAndAlias);
        }
    }

    /**
     * Returns details of joined tables.
     * 
     * @return Details of joined tables.
     */
    public JoinedTable getJoinedTable() {
        return m_joinedTable;
    }

    /**
     * Returns the table name.
     * 
     * @return Name of the table.
     */
    public TableNameAndAlias getTable() {
        return m_tableNameAndAlias;
    }
    
    /**
     * Gets a String representation of this table reference.
     *
     * @return String representation of this table reference.
     * 
     * @see Object#toString()
     */
    public String toString() {
        if (m_join) {
            return m_joinedTable.toString();
        } else {
            return m_tableNameAndAlias.toString();
        }
    }

    /**
     * Determines if this TableReference represents a JOIN.
     * 
     * @return <code>true</code> if this TableReference represents a JOIN. 
     */
    public boolean isJoin() {
        return m_join;
    }
}
