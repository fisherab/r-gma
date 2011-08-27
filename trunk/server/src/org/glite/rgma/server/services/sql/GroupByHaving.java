/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.util.ArrayList;
import java.util.List;


/**
 * GroupBy: an SQL GROUP BY...HAVING clause
 */
public class GroupByHaving {
    /** GROUP BY clause, a list of ExpSelConst objects. */
    private List<ExpressionOrConstant> m_groupBy;

    /** HAVING clause. */
    private ExpressionOrConstant m_having;

    /**
     * Creates a GROUP BY given a set of Expressions
     *
     * @param groupBy A vector of SQL Expressions (ExpSelConst objects).
     */
    public GroupByHaving(List<ExpressionOrConstant> groupBy) {
        m_groupBy = groupBy;
    }

    /**
     * Creates a new GroupByHaving object with the same attributes as <code>groupBy</code>.
     *
     * @param groupBy Object to copy.
     */
    public GroupByHaving(GroupByHaving groupBy) {
        m_groupBy = new ArrayList<ExpressionOrConstant>();

        if (groupBy.m_groupBy != null) {
            for (ExpressionOrConstant gb : groupBy.m_groupBy) {
                m_groupBy.add(ExpressionOrConstant.clone(gb));
            }
        }

        m_having = ExpressionOrConstant.clone(groupBy.m_having);
    }

    /**
     * Initializes the HAVING part of the GROUP BY.
     *
     * @param having An SQL Expression (the HAVING clause).
     */
    public void setHaving(ExpressionOrConstant having) {
        m_having = having;
    }

    /**
     * Gets the GROUP BY expressions
     *
     * @return A vector of SQL Expressions (ExpSelConst objects)
     */
    public List<ExpressionOrConstant> getGroupBy() {
        return m_groupBy;
    }

    /**
     * Gets the HAVING clause.
     *
     * @return An SQL expression.
     */
    public ExpressionOrConstant getHaving() {
        return m_having;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();

        if (m_groupBy != null) {
            buf.append("GROUP BY ");

            boolean first = true;

            for (ExpressionOrConstant gb : m_groupBy) {
                if (!first) {
                    buf.append(", ");
                } else {
                    first = false;
                }

                buf.append(gb);
            }
        }

        if (m_having != null) {
            buf.append(" HAVING " + m_having.toString());
        }

        return buf.toString();
    }
}
