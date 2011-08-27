/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.io.ByteArrayInputStream;

import java.util.Iterator;
import java.util.List;

import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.sql.parser.Parser;
import org.glite.rgma.server.services.sql.parser.TokenMgrError;

/**
 * An SQL CREATE INDEX statement.
 * 
 * CreateIndexStatement ::= CREATE INDEX index-name ON table-name ( column-name,
 * ... )
 */
public class CreateIndexStatement implements Statement {
	/** Name of index. */
	private String m_indexName;

	/** Name of table. */
	private String m_tableName;

	/** List of column names. */
	private List<String> m_columnNames;

	/**
	 * Creates a CREATE INDEX statement with a given index name.
	 * 
	 * @param indexName
	 *            Name of index to create.
	 */
	public CreateIndexStatement(String indexName) {
		m_indexName = indexName;
	}

	/**
	 * Sets the list of column names.
	 * 
	 * @param columns
	 *            The list of column names.
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
	 * @param tableName
	 *            The tableName to set
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
		if (!(obj instanceof CreateIndexStatement)) {
			return false;
		}
		CreateIndexStatement statement = (CreateIndexStatement) obj;

		if ((!statement.getIndexName().equals(getIndexName())) || (!statement.getTableName().equals(getTableName()))) {
			return false;
		}

		if (statement.getColumnNames().size() != getColumnNames().size()) {
			return false;
		}

		Iterator<String> statementColumnNames = statement.getColumnNames().iterator();
		for (String columnName : m_columnNames) {
			String statementColumnName = statementColumnNames.next();
			if (!columnName.equals(statementColumnName)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Parses the CREATE INDEX statement into a CreateIndexStatement object.
	 * 
	 * @param createStatement
	 *            SQL CREATE INDEX statement as a String.
	 * 
	 * @return SQL CREATE INDEX statement as a CreateIndexStatement.
	 * 
	 * @throws ParseException
	 *             Thrown if the CREATE INDEX statement is invalid.
	 */
	public static CreateIndexStatement parse(String createStatement) throws ParseException {
		Parser parser = new Parser(new ByteArrayInputStream((createStatement).getBytes()));
		CreateIndexStatement createIndexStatement;
		try {
			createIndexStatement = parser.CreateIndexStatement();
			String s = parser.getNextToken().toString();
			if (s.length() != 0) {
				throw new ParseException("Extra token after valid CREATE INDEX statement '" + s + "'");
			}
		} catch (TokenMgrError e) {
			throw new ParseException(e.getMessage());
		}
		return createIndexStatement;
	}

	/**
	 * Returns this CREATE INDEX statement as an SQL String.
	 * 
	 * @return This as a String.
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer("CREATE INDEX ");

		buf.append(m_indexName).append(" ON ").append(m_tableName).append(" (");

		int colNum = 0;
		for (String colName : m_columnNames) {
			buf.append(colName);
			if (colNum < m_columnNames.size() - 1) {
				buf.append(", ");
			}
			colNum++;
		}

		buf.append(')');

		return buf.toString();
	}

	/**
	 * Gets the index name.
	 * 
	 * @return Name of this index.
	 */
	public String getIndexName() {
		return m_indexName;
	}

	/**
	 * Sets the index name.
	 * 
	 * @param indexName
	 *            The name of this index.
	 */
	public void setIndexName(String indexName) {
		m_indexName = indexName;
	}
}
