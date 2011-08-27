/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;


/**
 * This bean objcet describes a colomn by name, operator and value
 * for use with a predicate
 *
 */
public class ColumnOperatorValue {
    /**
     * the name of the column
     */
    private String m_name;

    /**
     * the operator
     */
    private String m_operator;

    /**
     * the value of the column
     */
    private String m_value;

    /**
     * Creates a new ColumnOperatorValue object.
     *
     * @param name The name of the column
     * @param operator the operator of the colum value
     * @param value the value of the column
     */
    public ColumnOperatorValue(String name, String operator, String value) {
        m_name = name;
        m_operator = operator;
        m_value = value;
    }

    /**
     * get the name of the column
     * @return the name of the column
     */
    public String getName() {
        return m_name;
    }

    /**
     * get the operator of the column
     * @return the operator of the column
     */
    public String getOperator() {
        return m_operator;
    }

    /**
     * get the value of the column
     * @return the value of the column
     */
    public String getValue() {
        return m_value;
    }
}
