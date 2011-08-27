/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;


/**
 * An SQL query ORDER BY clause.
 */
public class OrderBy {
    /** Expression for ordering. */
    private ExpressionOrConstant m_expSelConst;

    /** Ascending order. */
    private boolean m_asc = true;

    /**
     * Creates a new OrderBy object.
     *
     * @param e Expression to order by.
     */
    public OrderBy(ExpressionOrConstant e) {
        m_expSelConst = e;
    }

    /**
     * Creates a new OrderBy with the same attributes as <code>ob</code>.
     *
     * @param ob OrderBy to copy.
     */
    public OrderBy(OrderBy ob) {
        m_expSelConst = ExpressionOrConstant.clone(ob.m_expSelConst);
        m_asc = ob.m_asc;
    }

    /**
     * Set the order to ascending or descending (defailt is ascending order).
     * @param a true for ascending order, false for descending order.
     */
    public void setAscOrder(boolean a) {
        m_asc = a;
    }

    /**
     * Get the order (ascending or descending)
     * @return true if ascending order, false if descending order.
     */
    public boolean getAscOrder() {
        return m_asc;
    }

    /**
     * Get the ORDER BY expression.
     * @return An expression (generally, a Constant that represents a column
     * name).
     */
    public ExpressionOrConstant getExpression() {
        return m_expSelConst;
    }

    /**
     * DOCUMENT ME!
     *
     * @return DOCUMENT ME!
     */
    public String toString() {
        return m_expSelConst.toString() + " " + (m_asc ? "ASC" : "DESC");
    }
}
;
