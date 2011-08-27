/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/**
 *
 */
package org.glite.rgma.server.services.sql;

import java.util.List;


/**
 * @author Stephen Hicks <s.j.c.hicks@rl.ac.uk>
 *
 */
public class JoinSpecification {
    /** Expression following ON. */
    private ExpressionOrConstant m_onExpression;

    /** List of column names for USING. */
    private List<String> m_usingColumnNames;

    /** Specification is an ON clause, not USING. */
    private boolean m_useON;

    /**
     * @param onExpression
     */
    public JoinSpecification(ExpressionOrConstant onExpression) {
        m_onExpression = onExpression;
        m_useON = true;
    }

    /**
     * @param usingColumnNames
     */
    public JoinSpecification(List<String> usingColumnNames) {
        m_usingColumnNames = usingColumnNames;
        m_useON = false;
    }

    public ExpressionOrConstant getOnExpression()
    {
        return m_onExpression;
    }

    public List<String> getUsingColumnNames()
    {
        return m_usingColumnNames;
    }

    public boolean usesOn()
    {
        return m_useON;
    }

    /**
     * DOCUMENT ME!
     *
     * @return DOCUMENT ME!
     */
    public String toString() {
        if (m_useON) {
            return "ON " + m_onExpression.toString();
        } else {
            if (m_usingColumnNames.size() == 0) {
                return "";
            }

            StringBuffer buf = new StringBuffer("USING (");
            boolean first = true;

            for (String colName : m_usingColumnNames) {
                if (!first) {
                    buf.append(", ");
                } else {
                    first = false;
                }

                buf.append(colName);
            }

            return buf.toString();
        }
    }
}
