/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.sql.parser.Parser;
import org.glite.rgma.server.services.sql.parser.TokenMgrError;

/**
 * An SQL CREATE TABLE statement. CreateTableStatement ::= CREATE TABLE name ( column_name column_type [modifiers], ...
 * ) column_type ::= INT | DATE | VARCHAR(n) | ... modifiers ::= PRIMARY KEY | NOT NULL | ...
 */
public class CreateTableStatement implements Statement {

	/** Name of table. */
	private String m_tableName;

	/** List of ColumnDefinition objects. */
	private List<ColumnDefinition> m_columns;

	/**
	 * Creates a CREATE TABLE statement on a given table
	 * 
	 * @param tableName
	 *            Name of table to create.
	 */
	public CreateTableStatement(String tableName) {
		m_tableName = tableName;
	}

	/**
	 * Sets the list of columns.
	 * 
	 * @param columns
	 *            The list of ColumnDefinition objects.
	 */
	public void setColumns(List<ColumnDefinition> columns) {
		m_columns = columns;
	}

	/**
	 * Returns the columns.
	 * 
	 * @return The list of ColumnDefinition objects.
	 */
	public List<ColumnDefinition> getColumns() {
		return m_columns;
	}

	/**
	 * Sets the primary key and not null flags for all columns based on the list of primary key column names. Case may
	 * vary so needs a search.
	 * 
	 * @param primaryKeyColumnNames
	 *            Column names that form the Primary Key.
	 * @throws ParseException
	 */
	public void setPrimaryKeyColumns(List<String> primaryKeyColumnNames) throws ParseException {
		Set<String> pkused = new HashSet<String>();
		for (ColumnDefinition columnDef : m_columns) {
			String columnName = columnDef.getName();
			boolean pk = false;
			for (String pkn : primaryKeyColumnNames) {
				if (pkn.equalsIgnoreCase(columnName)) {
					pk = true;
					pkused.add(pkn);
					break;
				}
			}
			columnDef.setPrimaryKey(pk);
			columnDef.setNotNull(pk);
		}
		for (String pkn : primaryKeyColumnNames) {
			if (!pkused.contains(pkn))
				throw new ParseException("Primary key '" + pkn + "' is not a column of the table");
		}
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
	 * @see Object#equals(java.lang.Object) Used by MySQLSchemaDatabase.createTable()
	 */
	public boolean equals(Object obj) {
		if (!(obj instanceof CreateTableStatement)) {
			return false;
		}

		CreateTableStatement cts = (CreateTableStatement) obj;

		// Compare table name
		if (!cts.getTableName().equals(this.getTableName())) {
			return false;
		}

		// Compare columns
		List<ColumnDefinition> ctsColumns = cts.getColumns();
		List<ColumnDefinition> thisColumns = this.getColumns();

		if (ctsColumns.size() != thisColumns.size()) {
			return false;
		}

		for (int i = 0; i < ctsColumns.size(); i++) {
			ColumnDefinition ctsColumn = ctsColumns.get(i);
			ColumnDefinition thisColumn = thisColumns.get(i);

			if (!ctsColumn.equals(thisColumn)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Parses the CREATE TABLE statement into a CreateTableStatement object.
	 * 
	 * @param createTableStatement
	 *            SQL CREATE TABLE statement as a String.
	 * @return SQL CREATE TABLE statement as a CreateTableStatement.
	 * @throws ParseException
	 *             Thrown if the CREATE TABLE statement is invalid.
	 */
	public static CreateTableStatement parse(String createTableStatement) throws ParseException {
		Parser parser = new Parser(new ByteArrayInputStream((createTableStatement).getBytes()));
		CreateTableStatement cts;
		try {
			cts = parser.CreateTableStatement();
			String s = parser.getNextToken().toString();
			if (s.length() != 0) {
				throw new ParseException("Extra token after valid CREATE TABLE statement '" + s + "'");
			}
		} catch (TokenMgrError e) {
			throw new ParseException(e.getMessage());
		}
		return cts;
	}

	/**
	 * Returns this CREATE TABLE statement as an SQL String.
	 * 
	 * @return This as a String.
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer("CREATE TABLE " + m_tableName);
		if ((m_columns != null) && (m_columns.size() > 0)) {
			buf.append(" (");
			// Count the number of primary key columns
			List<String> pkList = new Vector<String>();
			for (ColumnDefinition cd : m_columns) {
				if (cd.isPrimaryKey()) {
					pkList.add(cd.getName());
				}
			}
			int i = 0;
			for (ColumnDefinition cd : m_columns) {
				buf.append(cd.toString());
				if (i < (m_columns.size() - 1)) {
					buf.append(", ");
				} else {
					if (pkList.size() > 0) {
						buf.append(", PRIMARY KEY (");
						for (int pkColNum = 0; pkColNum < pkList.size(); pkColNum++) {
							buf.append(pkList.get(pkColNum));
							if (pkColNum < (pkList.size() - 1)) {
								buf.append(", ");
							}
						}
						buf.append(")");
					}
					buf.append(")");
				}
				i++;
			}
		}
		return buf.toString();
	}
}
