/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.io.ByteArrayInputStream;
import java.util.List;

import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.sql.parser.Parser;
import org.glite.rgma.server.services.sql.parser.TokenMgrError;


/**
 * Represents an SQL INSERT statement.
 *
 * Syntax is restricted to:
 * INSERT INTO <tablename> (<colname1>, <colname2>, ... ) VALUES ( <value1>, <value2>, ... );
 */
public class InsertStatement {
    /** Name of the table (INSERT INTO m_table...). */
    private TableName m_table;

    /** List of column names (String). */
    private List<String> m_columnNames = null;

    /** List of column values (Constant). */
    private List<Constant> m_columnValues = null;

    /**
     * Creates an INSERT statement on a given table
     */
    public InsertStatement(TableName table) {
        m_table = table;
    }

    /**
     * Returns the name of the table
     */
    public TableName getTableName() {
        return m_table;
    }

    /**
     * Sets the vector of column names
     *	
     * @param columnNames A vector of column names (String)
     */
    public void setColumnNames(List<String> columnNames) {
        m_columnNames = columnNames;
    }

    /**
     * Returns vector of column names if specified in the SQL.
     * If no vector was specified the return value is null.
     *
     * @return vector of String objects.
     */
    public List<String> getColumnNames() {
        return m_columnNames;
    }

    /**
     * Sets the list of VALUES for the INSERT statement
     *
     * @param columnValues A vector of Constant objects
     */
    public void setColumnValues(List<Constant> columnValues) {
        m_columnValues = columnValues;
    }

    /**
     * Returns the vector of column values for this insert statement
     */
    public List<Constant> getColumnValues() {
        return m_columnValues;
    }

    /**
     * Parses the INSERT statement into a InsertStatement object.
     *
     * @param insertStatement SQL INSERT statement as a String.
     *
     * @return SQL INSERT statement as a InsertStatement.
     *
     * @throws ParseException Thrown if the INSERT statement is invalid.
     */
	public static InsertStatement parse(String insertStatement) throws ParseException {
		Parser parser = new Parser(new ByteArrayInputStream((insertStatement).getBytes()));
		InsertStatement result;
		try {
			result = parser.InsertStatement();
			String s = parser.getNextToken().toString();
			if (s.length() != 0) {
				throw new ParseException("Extra token after valid INSERT statement '" + s + "'");
			}
		} catch (TokenMgrError e) {
			throw new ParseException(e.getMessage());
		}
		if (result.m_columnNames.size() != result.m_columnValues.size()) {
			throw new ParseException("INSERT statement '" + insertStatement + "' does not have same number of columns and values.");
		}
		return result;
	}

    /**
     * Converts this statement into a String.
     * As:
     *      INSERT INTO m_table (m_columnNames, ...) VALUES (m_columnValues, ...);
     */
    public String toString() {
        StringBuffer buf = new StringBuffer("INSERT INTO " + m_table);

        if ((m_columnNames != null) && (m_columnNames.size() > 0)) {
            buf.append("(").append(m_columnNames.get(0));

            for (int i = 1; i < m_columnNames.size(); i++) {
                buf.append(",").append(m_columnNames.get(i));
            }

            buf.append(")");
        }

        buf.append(" ");

        if (getColumnValues() != null) {
            buf.append("VALUES ");

            buf.append("(").append(m_columnValues.get(0).toString());

            for (int i = 1; i < m_columnValues.size(); i++) {
                buf.append(",").append(m_columnValues.get(i).toString());
            }

            buf.append(");");
        }

        return buf.toString();
    }

    /**
     * Sets the name of the table for this insert.
     * 
     * @param table Table name to set.
     */
    public void setTable(TableName table) {
        m_table = table;
    }
}
