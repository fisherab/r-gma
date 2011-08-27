/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.io.ByteArrayInputStream;
import java.util.Map;

import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.sql.parser.Parser;
import org.glite.rgma.server.services.sql.parser.TokenMgrError;


/**
 * An SQL UPDATE statement.
 * 
 * NB: The order of the assignments in the SET clause is not
 * maintained in this object.
 */
public class UpdateStatement {
    /** WHERE clause. */
    private ExpressionOrConstant m_where;

    /** Set clauses, mapping from column name to update value. */
    private Map<String,ExpressionOrConstant> m_set;

    /** Name of table to update. */
    private TableName m_tableName;

    /**
     * Create an UPDATE statement on a given table.
     *
     * @param tableName Name of table.
     */
    public UpdateStatement(TableName tableName) {
        m_tableName = tableName;
    }

    /**
     * Get the SQL expression that specifies a given column's update value.
     *
     * @param col The column name.
     *
     * @return a ExpSelConst, like a Constant representing a value, or a more
     *         complex SQL expression.
     */
    public ExpressionOrConstant getColumnUpdate(String col) {
        return m_set.get(col);
    }

    /**
     * Get the whole SET... clause
     *
     * @return A Map, where keys are column names (the columns to
     *         update), and values are ExpSelConst objects (Expressions that
     *         specify column values).
     */
    public Map<String,ExpressionOrConstant> getSet() {
        return m_set;
    }

    /**
     * Gets name of table to update.
     *
     * @return Name of table to update.
     */
    public TableName getTableName() {
        return m_tableName;
    }

    /**
     * Get the WHERE clause of this UPDATE statement.
     *
     * @return An SQL Expression compatible with a WHERE... clause.
     */
    public ExpressionOrConstant getWhere() {
        return m_where;
    }

    /**
     * Insert a SET clause in the UPDATE statement
     *
     * @param set A Map, where keys are column names (the columns to
     *        update), and values are ExpSelConst objects (the column values).
     */
    public void setSet(Map<String,ExpressionOrConstant> set) {
        m_set = set;
    }

    /**
     * Insert a WHERE clause in the UPDATE statement
     *
     * @param where An SQL Expression compatible with a WHERE... clause.
     */
    public void setWhere(ExpressionOrConstant where) {
        m_where = where;
    }
    
    /**
     * Parses the UPDATE statement into a UpdateStatement object.
     *
     * @param updateStatement SQL UPDATE statement as a String.
     *
     * @return SQL UPDATE statement as a UpdateStatement.
     *
     * @throws ParseException Thrown if the UPDATE statement is invalid.
     */
    public static UpdateStatement parse(String updateStatement)
        throws ParseException {
        Parser parser = new Parser(new ByteArrayInputStream((updateStatement).getBytes()));
        UpdateStatement upStat;
		try {
			upStat = parser.UpdateStatement();
			String s = parser.getNextToken().toString();
			if (s.length() != 0) {
				throw new ParseException("Extra token after valid UPDATE statement '" + s + "'");
			}
		} catch (TokenMgrError e) {
			throw new ParseException(e.getMessage());
		}
	
        return upStat;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        StringBuffer buf = new StringBuffer("UPDATE ");
        buf.append(m_tableName);
        buf.append(" SET ");

        boolean first = true;

        for (String key : m_set.keySet()) {
            if (!first) {
                buf.append(", ");
            } else {
                first = false;
            }

            buf.append(key).append(" = ").append(m_set.get(key));
        }

        if (m_where != null) {
            buf.append(" WHERE " + m_where.toString());
        }

        return buf.toString();
    }
}
