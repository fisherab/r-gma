/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.io.ByteArrayInputStream;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.sql.parser.Parser;
import org.glite.rgma.server.services.sql.parser.TokenMgrError;

import java.util.Iterator;
import java.util.List;

/**
 * An SQL CREATE VIEW statement.
 *
 * CreateViewStatement ::= CREATE VIEW view-name AS SELECT column-name, ... FROM table-name
 */
public class CreateViewStatement implements Statement {
    /** Name of view. */
    private String m_viewName;
    
    /** Name of table. */
    private String m_tableName;

    /** List of column names. */
    private List<String> m_columnNames;

    /**
     * Creates a CREATE VIEW statement with a given view name.
     *
     * @param viewName Name of view to create.
     */
    public CreateViewStatement(String viewName) {
        m_viewName = viewName;
    }

    /**
     * Sets the list of column names.
     *
     * @param columns The list of column names.
     */
    public void setColumnNames(List<String> columns) {
        m_columnNames = columns;
    }

    /**
     * Returns the columns names.
     *
     * @return The list of column names.
     */
    public List<String> getColumnNames() {
        return m_columnNames;
    }

    /**
     * Sets the tableName.
     *
     * @param tableName The tableName to set
     */
    public void setTableName(String tableName) {
        m_tableName = tableName;
    }

    /**
     * Returns the tableName.
     *
     * @return The table name.
     */
    public String getTableName() {
        return m_tableName;
    }

    /**
     * @see Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof CreateViewStatement)) {
            return false;
        }
        
        CreateViewStatement statement = (CreateViewStatement)obj;
        
        if ( (!statement.getViewName().equals(getViewName())) || (!statement.getTableName().equals(getTableName()) )) {
            return false;
        }
        
        if (statement.getColumnNames().size() != getColumnNames().size()) {
            return false;
        }
        
        Iterator<String> statementColumnNames = statement.getColumnNames().iterator();
        for (String columnName  : m_columnNames) {
            String statementColumnName = statementColumnNames.next();
            if (!columnName.equals(statementColumnName)) {
                return false;
            }
        }
            
        
        return true;
    }

    /**
     * Parses the CREATE VIEW statement into a CreateViewStatement object.
     *
     * @param createViewStatement SQL CREATE VIEW statement as a String.
     *
     * @return SQL CREATE VIEW statement as a CreateViewStatement.
     *
     * @throws ParseException Thrown if the CREATE VIEW statement is invalid.
     */
    public static CreateViewStatement parse(String createViewStatement)
        throws ParseException {
        Parser parser = new Parser(new ByteArrayInputStream((createViewStatement).getBytes()));
        CreateViewStatement cvs; 
        try {
			cvs = parser.CreateViewStatement();
			String s = parser.getNextToken().toString();
			if (s.length() != 0) {
				throw new ParseException("Extra token after valid CREATE VIEW statement '" + s + "'");
			}
		} catch (TokenMgrError e) {
			throw new ParseException(e.getMessage());
		}
        return cvs;
    }

    /**
     * Returns this CREATE VIEW statement as an SQL String.
     *
     * @return This as a String.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer("CREATE VIEW ");
        
        buf.append(m_viewName).append(" AS SELECT ");
        
        int colNum = 0;
        for (String colName : m_columnNames) {            
            buf.append(colName);
            if (colNum < m_columnNames.size() - 1) {
                buf.append(", ");
            }
            colNum++;
        }
        
        buf.append(" FROM ").append(m_tableName);
        
        return buf.toString();
    }

    /**
     * Gets the index name.
     * 
     * @return Name of this index.
     */
    public String getViewName() {
        return m_viewName;
    }

    /**
     * Sets the index name.
     * 
     * @param indexName The name of this index.
     */
    public void setViewName(String indexName) {
        m_viewName = indexName;
    }
}
