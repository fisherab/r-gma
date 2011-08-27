/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;


/**
 * TODO: Add ON or USING.
 *
 */
public class JoinedTable {
    /** First table reference. */
    private TableReference m_tableRef1;

    /** Second table reference. */
    private TableReference m_tableRef2;

    /** NATURAL flag. */
    private boolean m_natural;

    /** Type of join (INNER, OUTER, UNION). */
    private JoinType m_joinType;

    /** Type of outer join (LEFT, RIGHT, FULL). */
    private OuterJoinType m_outerJoinType;

    /** JOIN specification (ON or USING). Can be null. */
    private JoinSpecification m_joinSpecification;

    /**
     * Creates a new JoinedTable object.
     *
     * @param ref1 First table reference.
     * @param ref2 Second table reference.
     * @param natural NATURAL flag.
     * @param joinSpecification JOIN specification.
     * @param joinType Type of join (INNER, OUTER, UNION).
     */
    public JoinedTable(TableReference ref1, TableReference ref2, boolean natural, JoinSpecification joinSpecification, JoinType joinType) {
        m_tableRef1 = ref1;
        m_tableRef2 = ref2;
        m_natural = natural;
        m_joinSpecification = joinSpecification;
        m_joinType = joinType;
    }

    /**
     * Creates a new JoinedTable object for an OUTER join.
     *
     * @param ref1 First table reference.
     * @param ref2 Second table reference.
     * @param natural NATURAL flag.
     * @param outerJoinType Type of outer join (LEFT, RIGHT, FULL).
     */
    public JoinedTable(TableReference ref1, TableReference ref2, boolean natural, JoinSpecification joinSpecification, OuterJoinType outerJoinType) {
        this(ref1, ref2, natural, joinSpecification, JoinType.OUTER);
        m_outerJoinType = outerJoinType;
    }

    /**
     * Creates a new JoinedTable with the same attributes as <code>joinedTable</code>.
     * 
     * @param joinedTable JoinedTable to copy.
     */
    public JoinedTable(JoinedTable joinedTable) {
        m_tableRef1 = new TableReference(joinedTable.m_tableRef1);
        m_tableRef2 = new TableReference(joinedTable.m_tableRef2);
        m_natural = joinedTable.m_natural;
        m_joinSpecification = joinedTable.m_joinSpecification;
        m_joinType = joinedTable.m_joinType;
        m_outerJoinType = joinedTable.m_outerJoinType;
    }

    /**
     * Gets the JOIN type.
     *
     * @return The JOIN type.
     */
    public JoinType getJoinType() {
        return m_joinType;
    }

    /**
     * Determines if it is a NATURAL JOIN.
     *
     * @return <code>true</code> if it is a NATURAL JOIN.
     */
    public boolean isNatural() {
        return m_natural;
    }

    /**
     * Gets the type of OUTER JOIN.
     *
     * @return Type of the OUTER JOIN, or null if not applicable.
     */
    public OuterJoinType getOuterJoinType() {
        return m_outerJoinType;
    }

    /**
     * Gets the first table reference.
     *
     * @return The first table reference.
     */
    public TableReference getTableRef1() {
        return m_tableRef1;
    }

    /**
     * Gets the second table reference.
     *
     * @return The second table reference.
     */
    public TableReference getTableRef2() {
        return m_tableRef2;
    }

    /**
     * Sets the first table reference.
     *
     * @param tableRef1 First table reference.
     */
    public void setTableRef1(TableReference tableRef1) {
        m_tableRef1 = tableRef1;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append(m_tableRef1);

        if (m_natural) {
            buf.append(" NATURAL ");
        }

        if (m_joinType == JoinType.OUTER) {
            buf.append(m_outerJoinType).append(" OUTER");
        } else {
            buf.append(m_joinType);
        }

        buf.append(" JOIN ");

        buf.append(m_tableRef2);

        if (m_joinSpecification != null) {
            buf.append(' ').append(m_joinSpecification.toString());
        }

        return buf.toString();
    }

    /**
     * @return Returns the joinSpecification.
     */
    public JoinSpecification getJoinSpecification() {
        return m_joinSpecification;
    }

    /** Type of JOIN. */
    public enum JoinType {INNER, 
        OUTER, 
        UNION;
    }

    /** Type of OUTER JOIN. */
    public enum OuterJoinType {LEFT, 
        RIGHT, 
        FULL;
    }
}
