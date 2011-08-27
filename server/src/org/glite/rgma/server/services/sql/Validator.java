/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.glite.rgma.server.services.sql.DataType.Type;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.SchemaColumnDefinition;
import org.glite.rgma.server.system.SchemaTableDefinition;

/**
 * Validates SQL statements against schema table definitions.
 */
public class Validator {

	/** Mapping from table name to table definition */
	private final Map<String, SchemaTableDefinition> m_tableDefs;

	/** Mapping from aliases to real table names */
	private final Map<String, String> m_aliases;

	/** If not null, a select statement to be validated. */
	private SelectStatement m_select;

	/**
	 * Construct a validator to validate a SelectStatement.
	 */
	public Validator(SelectStatement select, Map<String, SchemaTableDefinition> tableDefs) throws RGMAPermanentException, RGMAPermanentException {
		m_tableDefs = new HashMap<String, SchemaTableDefinition>();
		m_aliases = new HashMap<String, String>();
		m_select = select;

		List<TableNameAndAlias> tables = select.getTables();
		for (TableNameAndAlias table : tables) {
			String vdbTableName = table.getVdbTableName();
			SchemaTableDefinition tableDef = tableDefs.get(vdbTableName);
			if (tableDef == null) {
				throw new RGMAPermanentException("No table definition for table: " + vdbTableName);
			}
			m_tableDefs.put(vdbTableName, tableDef);
			if (table.getAlias() != null) {
				m_aliases.put(table.getAlias().toUpperCase(), vdbTableName);
			}
		}
	}

	public void validate() throws RGMAPermanentException, RGMAPermanentException {
		if (m_select != null) {
			validateSelect(m_select);
		}
	}

	/**
	 * Validate a SELECT statement. Performs the following checks:
	 * <ul>
	 * <li>Checks that SELECT * is not used in conjunction with a join</li>
	 * <li>Checks that all column names are only specified once (this is necessary for the creation of temporary tables
	 * in one-time queries and is not actually invalid SQL)</li>
	 * <li>Checks that all tables and columns referenced in SELECT, FROM and WHERE exist in the schema tables specified</li>
	 * <li>Checks that the types of expressions in the predicate match</li>
	 * 
	 * @param handler
	 *            Parsed SELECT query
	 * @param tableDefs
	 *            mapping of table names (in upper case) to TableDefinition objects for tables which may appear in the
	 *            select statement.
	 */
	private void validateSelect(SelectStatement select) throws RGMAPermanentException {
		// Validate ON/USING clause in joins
		for (TableReference fromItem : select.getFrom()) {
			if (fromItem.isJoin()) {
				JoinedTable join = fromItem.getJoinedTable();
				JoinSpecification spec = join.getJoinSpecification();
				if (spec != null) {
					if (spec.usesOn()) {
						ExpressionOrConstant on = spec.getOnExpression();
						if (on instanceof Expression) {
							validateExpression((Expression) on);
						} else if (on instanceof Constant) {
							validateConstant((Constant) on);
						}
					} else {
						List<String> using = spec.getUsingColumnNames();
						TableReference table1 = join.getTableRef1();
						TableReference table2 = join.getTableRef2();
						for (String usingColumn : using) {
							// Check column exists in both tables
							ColumnName col1 = new ColumnName(table1.getTable().getVdbName(), table1.getTable().getTableName(), usingColumn);
							validateColumn(col1);
							ColumnName col2 = new ColumnName(table2.getTable().getVdbName(), table2.getTable().getTableName(), usingColumn);
							validateColumn(col2);
						}
					}
				}
			}
		}

		// Validate the selected columns
		List<SelectItem> selectedColumns = select.getSelect();
		List<String> colNames = new ArrayList<String>();
		for (SelectItem item : selectedColumns) {

			// Check for duplicate columns
			String actualName;
			if (item.getAlias() != null) {
				actualName = item.getAlias();
				/* TODO this is not right ... */
				// } else if (item.getColumnName() != null) {
				// actualName = item.getColumnName();
			} else {
				actualName = item.toString();
			}

			if (colNames.contains(actualName)) {
				throw new RGMAPermanentException("Duplicate column names in select: \"" + actualName + "\".");
			} else {
				colNames.add(actualName);
			}

			// Validate select item
			ExpressionOrConstant exp = item.getExpression();
			if (exp instanceof Expression) {
				validateExpression((Expression) exp);
			} else {
				validateConstant((Constant) exp);
			}
		}

		// Validate the predicate
		ExpressionOrConstant where = select.getWhere();
		if (where instanceof Expression) {
			validateExpression((Expression) where);
		}

		// Validate ORDER BY
		List<OrderBy> orderByItems = select.getOrderBy();
		if (orderByItems != null) {
			for (OrderBy orderBy : orderByItems) {
				ExpressionOrConstant exp = orderBy.getExpression();
				if (exp instanceof Expression) {
					validateExpression((Expression) exp);
				} else if (exp instanceof Constant) {
					validateConstant((Constant) exp);
				}
			}
		}

		// Validate GROUP BY and HAVING
		GroupByHaving groupBy = select.getGroupBy();
		if (groupBy != null) {
			List<ExpressionOrConstant> expressions = groupBy.getGroupBy();
			if (expressions != null) {
				for (ExpressionOrConstant exp : expressions) {
					if (exp instanceof Expression) {
						validateExpression((Expression) exp);
					} else if (exp instanceof Constant) {
						validateConstant((Constant) exp);
					}
				}
			}
			ExpressionOrConstant having = groupBy.getHaving();
			if (having != null) {
				if (having instanceof Expression) {
					validateExpression((Expression) having);
				} else if (having instanceof Constant) {
					validateConstant((Constant) having);
				}
			}
		}
	}

	/**
	 * Check an SQL Expression is valid. Checks that the columns referenced exist in the tables specified and that the
	 * types of the operands all match.
	 * 
	 * @param expression
	 *            SQL Expression to check
	 * @param aliases
	 *            Mapping from aliases to table names for the SQL query
	 * @return The type of the expression, one of Constant.Type.STRING, Constant.Type.NUMBER, Constant.Type.UNKNOWN or
	 *         Constant.Type.NULL.
	 */
	private Constant.Type validateExpression(Expression expression) throws RGMAPermanentException {
		List<ExpressionOrConstant> operands = expression.getOperands();
		Constant.Type[] types = new Constant.Type[operands.size()];
		int op = 0;

		/* Deal with count() */
		String agg = expression.getAggregate();
		if (agg != null && agg.toUpperCase().equals("COUNT")) {
			ExpressionOrConstant operand = operands.get(0);
			if (operand instanceof Expression) {
				validateExpression((Expression) operand);
			} else if (operand instanceof Constant) {
				validateConstant((Constant) operand);
			}
			return Constant.Type.NUMBER;
		}

		for (ExpressionOrConstant operand : operands) {
			if (operand instanceof Expression) {
				types[op] = validateExpression((Expression) operand);
			} else if (operand instanceof Constant) {
				types[op] = validateConstant((Constant) operand);
			} else {
				// Don't validate anything else (e.g. embedded SELECT) yet
				types[op] = Constant.Type.UNKNOWN;
			}
			op++;
		}

		// Check types are equal, ignoring UNKNOWN or NULL types
		Constant.Type type = Constant.Type.UNKNOWN;
		for (int i = 0; i < types.length; i++) {
			if (type == Constant.Type.UNKNOWN) {
				if (types[i] != Constant.Type.NULL) {
					type = types[i];
				}
			} else {
				if ((types[i] != Constant.Type.NULL) && (types[i] != Constant.Type.UNKNOWN) && (types[i] != type)) {
					throw new RGMAPermanentException("Types do not match in expression: " + expression);
				}
			}
		}

		String operator = expression.getOperator().trim().toUpperCase();
		if (operator.equals("AND") || operator.equals("OR") || operator.equals("NOT") || operator.equals("IN") || operator.equals("BETWEEN")
				|| operator.equals("LIKE") || operator.equals("=") || operator.equals(">") || operator.equals("<") || operator.equals(">=")
				|| operator.equals("<=") || operator.equals("<>") || operator.equals("!=") || operator.equals("IS NULL") || operator.equals("IS NOT NULL")
				|| operator.equals("EXISTS")) {
			// Need to return a boolean type really
			return Constant.Type.UNKNOWN;
		} else {
			return type;
		}
	}

	/**
	 * Check an SQL Constant is valid. If the constant refers to a column, check that the column exists in the specified
	 * tables.
	 * 
	 * @param constant
	 *            SQL Constant to check
	 * @param aliases
	 *            Mapping from aliases to table names for the SQL query
	 * @return The type of the constant, one of Constant.Type.STRING, Constant.Type.NUMBER, Constant.Type.UNKNOWN or
	 *         Constant.Type.NULL.
	 */
	private Constant.Type validateConstant(Constant constant) throws RGMAPermanentException {
		if (constant.getType() == Constant.Type.COLUMN_NAME) {
			ColumnName item = new ColumnName(constant.getValue());
			return validateColumn(item);
		} else {
			return constant.getType();
		}
	}

	/**
	 * Check an SQL column name is valid. Checks that the column exists in one and only one of the specified tables.
	 * 
	 * @param column
	 *            Name of column to check
	 * @param table
	 *            Name of table the column is part of - may be an alias or null if unspecified.
	 * @param aliases
	 *            Mapping from aliases to table names for the SQL query
	 * @return The type of the constant, one of Constant.Type.STRING, Constant.Type.NUMBER, Constant.Type.UNKNOWN or
	 *         Constant.Type.NULL.
	 */
	private Constant.Type validateColumn(ColumnName colItem) throws RGMAPermanentException {
		String column = colItem.getColumnName();
		if (column.equals("COUNT(*)")) {
			return Constant.Type.UNKNOWN;
		}
		String vdbTableName = colItem.getVdbTableName();

		Constant.Type result = Constant.Type.UNKNOWN;

		if (vdbTableName == null) {
			// No table declared for column - check it exists in one (and only one) table definition
			int matches = 0;
			for (SchemaTableDefinition tableDef : m_tableDefs.values()) {
				Constant.Type type = getColumnType(column, tableDef);
				if (type != Constant.Type.UNKNOWN) {
					result = type;
					matches++;
				}
			}
			if (matches == 0) {
				throw new RGMAPermanentException("Column '" + column + "' does not exist in any specified tables: " + m_select);
			} else if (matches > 1) {
				throw new RGMAPermanentException("Column '" + column + "' exists in more than one specified tables: " + m_select);
			}
			return result;

		} else {
			/*
			 * Table specified - check table and column exist - but missing VDBname cannot be taken as "DEFAULT" as in
			 * "Select T1.C, V2.T1.C from V1.T1, V2.T1"
			 */
			SchemaTableDefinition matchingTable = m_tableDefs.get(vdbTableName);

			/* This wil match if column specified as V.T.C */
			if (matchingTable != null) {
				result = getColumnType(column, matchingTable);
				if (result == Constant.Type.UNKNOWN) {
					throw new RGMAPermanentException("Column '" + column + "' does not exist in table '" + vdbTableName + "'");
				}
				return result;
			}

			/* Look for T.C matches */
			int matches = 0;
			for (SchemaTableDefinition tableDef : m_tableDefs.values()) {
				if (tableDef.getTableName().toUpperCase().equals(vdbTableName)) {
					Constant.Type type = getColumnType(column, tableDef);
					if (type != Constant.Type.UNKNOWN) {
						result = type;
						matches++;
					}
				}
			}
			if (matches == 1) {
				return result;
			}
			if (matches > 1) {
				throw new RGMAPermanentException("Column '" + column + "' exists in more than one specified tables: " + m_select);
			}

			/* Finally try as an alias */
			String actualTableName = m_aliases.get(vdbTableName);
			if (actualTableName != null) {
				matchingTable = m_tableDefs.get(actualTableName.toUpperCase());
			}
			if (matchingTable == null) {
				throw new RGMAPermanentException("Table '" + vdbTableName + "' not declared in SELECT");
			}
			result = getColumnType(column, matchingTable);
			if (result == Constant.Type.UNKNOWN) {
				throw new RGMAPermanentException("Column '" + column + "' does not exist in table '" + vdbTableName + "'");
			}
			return result;
		}
	}

	/**
	 * Get the type of a colum in a table.
	 * 
	 * @param column
	 *            Name of column to check
	 * @param tableDef
	 *            Table definition the column should be present in.
	 * @return The type of the constant, Constant.Type.STRING, Constant.Type.NUMBER. Returns Constant.Type.UNKNOWN only
	 *         if the column was not found in the table.
	 */
	private Constant.Type getColumnType(String column, SchemaTableDefinition tableDef) throws RGMAPermanentException {
		SchemaColumnDefinition columnDef = null;
		for (SchemaColumnDefinition col : tableDef.getColumns()) {
			if (col.getName().equalsIgnoreCase(column)) {
				columnDef = col;
				break;
			}
		}

		if (columnDef == null) {
			return Constant.Type.UNKNOWN;
		}

		Type type = columnDef.getType().getType();
		switch (type) {
		case INTEGER:
		case REAL:
		case DOUBLE_PRECISION:
			return Constant.Type.NUMBER;
		case CHAR:
		case VARCHAR:
		case DATE:
		case TIME:
		case TIMESTAMP:
			return Constant.Type.STRING;
		default:
			return Constant.Type.UNKNOWN;
		}
	}
}
