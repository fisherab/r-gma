/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.sql.parser.Parser;
import org.glite.rgma.server.services.sql.parser.TokenMgrError;
import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * SelectStatement: an SQL SELECT statement
 * 
 * <pre>
 * SelectStatement ::= SelectWithoutOrder [OrderByClause] 
 * SelectWithoutOrder ::= SELECT [ ALL | DISTINCT ] SelectList FromClause [ WhereClause ] [ GroupByClause ] 
 * SelectList ::= * | COUNT(*) | selectItem (, selectItem)* | COUNT &quot;(&quot; [ DISTINCT | ALL ] ObjectName &quot;)&quot; 
 * SelectItem ::= SQLSimpleExpression() [ [AS] Alias ] 
 * FromClause ::= TableReference (, TableReference)* 
 * TableReference ::= ... 
 * WhereClause ::= WHERE SQLExpression
 * GroupByClause ::= GROUP BY SQLExpressionList [ HAVING SQLExpression ] 
 * OrderByClause ::= ORDER BY SQLSimpleExpression [ ASC | DESC ] (, SQLSimpleExpression [ ASC | DESC ] )*
 * </pre>
 */
public class SelectStatement implements Statement {
	/** SQL WHERE clause. */
	private ExpressionOrConstant m_whereClause;

	/** SQL GROUP BY clause. */
	private GroupByHaving m_groupBy;

	/** SQL FROM elements. */
	private List<TableReference> m_from;

	/** SQL ORDER BY elements. */
	private List<OrderBy> m_orderBy;

	/** SQL SELECT elements. */
	private List<SelectItem> m_select;

	/** DISTINCT flag. */
	private boolean m_distinct;

	/**
	 * Create a new SELECT statement
	 */
	public SelectStatement() {}

	/**
	 * Creates a copy of the given SELECT statement.
	 * 
	 * @param query
	 *            SELECT to copy.
	 */
	public SelectStatement(SelectStatement query) {
		m_whereClause = ExpressionOrConstant.clone(query.m_whereClause);

		if (query.m_groupBy != null) {
			m_groupBy = new GroupByHaving(query.m_groupBy);
		}

		m_from = new ArrayList<TableReference>();

		for (TableReference tr : query.m_from) {
			m_from.add(new TableReference(tr));
		}

		if (query.m_orderBy != null) {
			m_orderBy = new ArrayList<OrderBy>();

			for (OrderBy ob : query.m_orderBy) {
				m_orderBy.add(new OrderBy(ob));
			}
		}

		m_select = new ArrayList<SelectItem>();

		for (SelectItem si : query.m_select) {
			m_select.add(new SelectItem(si));
		}

		m_distinct = query.m_distinct;
	}

	/**
	 * Parses the SELECT statement into a SelectStatement object.
	 * 
	 * @param selectStatement
	 *            SQL SELECT statement as a String.
	 * @return SQL SELECT statement as a SelectStatement.
	 * @throws ParseException
	 *             Thrown if the SELECT statement is invalid.
	 */
	public static SelectStatement parse(String selectStatement) throws ParseException {
		Parser parser = new Parser(new ByteArrayInputStream((selectStatement).getBytes()));
		SelectStatement queryStatement;
		try {
			queryStatement = parser.QueryStatement();
			String s = parser.getNextToken().toString();
			if (s.length() != 0) {
				throw new ParseException("Extra token after valid SELECT statement '" + s + "'");
			}
		} catch (TokenMgrError e) {
			throw new ParseException(e.getMessage());
		}
		return queryStatement;
	}

	/**
	 * Gets the distinct flag.
	 * 
	 * @return true if it is a SELECT DISTINCT query, false otherwise.
	 */
	public boolean isDistinct() {
		return m_distinct;
	}

	/**
	 * Sets the distinct flag to the specified value.
	 * 
	 * @param distinct
	 *            Distinct flag value.
	 */
	public void setDistinct(boolean distinct) {
		m_distinct = distinct;
	}

	/**
	 * Get the FROM part of the statement
	 * 
	 * @return A vector of TableReference objects
	 */
	public List<TableReference> getFrom() {
		return m_from;
	}

	/**
	 * Get the GROUP BY...HAVING part of the statement
	 * 
	 * @return A GROUP BY...HAVING clause
	 */
	public GroupByHaving getGroupBy() {
		return m_groupBy;
	}

	/**
	 * Get the ORDER BY clause
	 * 
	 * @return DOCUMENT ME!
	 */
	public List<OrderBy> getOrderBy() {
		return m_orderBy;
	}

	/**
	 * Get the SELECT part of the statement
	 * 
	 * @return A vector of ZSelectItem objects
	 */
	public List<SelectItem> getSelect() {
		return m_select;
	}

	/**
	 * Get the WHERE part of the statement
	 * 
	 * @return An SQL Expression or sub-query (Expression or SelectStatement object)
	 */
	public ExpressionOrConstant getWhere() {
		return m_whereClause;
	}

	/**
	 * Insert the FROM part of the statement
	 * 
	 * @param from
	 *            a Vector of TableReference objects
	 */
	public void addFrom(List<TableReference> from) {
		m_from = from;
	}

	/**
	 * Insert a GROUP BY...HAVING clause
	 * 
	 * @param g
	 *            A GROUP BY...HAVING clause
	 */
	public void addGroupBy(GroupByHaving g) {
		m_groupBy = g;
	}

	/**
	 * Insert an ORDER BY clause
	 * 
	 * @param v
	 *            A vector of OrderBy objects
	 */
	public void addOrderBy(List<OrderBy> v) {
		m_orderBy = v;
	}

	/**
	 * Insert the SELECT part of the statement
	 * 
	 * @param s
	 *            A vector of ZSelectItem objects
	 */
	public void addSelect(List<SelectItem> s) {
		m_select = s;
	}

	/**
	 * Insert a WHERE clause
	 * 
	 * @param w
	 *            An SQL Expression
	 */
	public void addWhere(ExpressionOrConstant w) {
		m_whereClause = w;
	}

	/**
	 * Gets a String representation of this SELECT statement.
	 * 
	 * @return String representation of this SELECT statement.
	 * @see Object#toString()
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer("SELECT ");

		if (m_distinct) {
			buf.append("DISTINCT ");
		}

		boolean first = true;

		for (SelectItem select : m_select) {
			if (!first) {
				buf.append(", ");
			} else {
				first = false;
			}

			buf.append(select);
		}

		buf.append(" FROM ");

		first = true;

		for (TableReference tableRef : m_from) {
			if (!first) {
				buf.append(", ");
			} else {
				first = false;
			}

			buf.append(tableRef);
		}

		if (m_whereClause != null) {
			buf.append(" WHERE " + m_whereClause.toString());
		}

		if (m_groupBy != null) {
			buf.append(" " + m_groupBy.toString());
		}

		if (m_orderBy != null) {
			buf.append(" ORDER BY ");
			first = true;

			for (OrderBy orderBy : m_orderBy) {
				if (!first) {
					buf.append(", ");
				} else {
					first = false;
				}

				buf.append(orderBy);
			}
		}
		return buf.toString();
	}

	/**
	 * Check that the query is simple
	 * 
	 * @throws RGMAPermanentException
	 *             if the query is not a simple query
	 */
	public void checkQueryIsSimple() throws RGMAPermanentException {
		// no joins
		if ((m_from.size() != 1) || (m_from.get(0).isJoin())) {
			throw new RGMAPermanentException("Query references more than one table and may not be used in this context");
		}

		// has a distinct clause
		if (m_distinct) {
			throw new RGMAPermanentException("Query contains IS DISTINCT and may not be used in this context");
		}

		// no GROUP BY
		if (m_groupBy != null) {
			throw new RGMAPermanentException("Query contains GROUP BY and may not be used in this context");
		}

		// no ORDER BY
		if (m_orderBy != null) {
			throw new RGMAPermanentException("Query contains ORDER BY and may not be used in this context");
		}

		if (m_whereClause != null) {
			if (m_whereClause instanceof Expression) {
				((Expression) m_whereClause).checkExpressionIsSimple();
			} else {
				throw new RGMAPermanentException("WHERE clause is not simple: " + m_whereClause);
			}
		}

		for (SelectItem item : m_select) {
			if (item.getAggregate() != null) {
				throw new RGMAPermanentException("Query contains aggregation: " + item.getAggregate().toUpperCase());
			}

			if (item.isCountDistinct()) {
				throw new RGMAPermanentException("Query contains COUNT DISTINCT: " + item);
			}
		}
	}

	/**
	 * Test if this SelectStatement represents a simple query. A 'simple' query:
	 * <ul>
	 * <li>References only one table</li>
	 * <li>Contains no embedded SELECTS</li>
	 * <li>Contains no DISTINCT</li>
	 * <li>Contains no GROUP BY</li>
	 * <li>Contains no ORDER BY</li>
	 * <li>Contains no UNION, INTERSECT or MINUS</li>
	 * <li>Contains no aggregation (AVG, SUM, MAX, MIN)</li>
	 * <li>Contains no complex operators (OR, IN, BETWEEN etc.)</li>
	 * </ul>
	 * 
	 * @return <code>true</code> if this SelectStatement is simple, <code>false</code> otherwise.
	 */
	public boolean isSimpleQuery() {
		try {
			checkQueryIsSimple();
		} catch (RGMAPermanentException e) {
			return false;
		}
		return true;
	}

	/**
	 * Get the names of all tables referenced in this SelectStatement.
	 * 
	 * @return List of AliasedName objects.
	 */
	public List<TableNameAndAlias> getTables() {
		List<TableNameAndAlias> tables = new ArrayList<TableNameAndAlias>();
		addTablesFromSelect(tables);
		return tables;
	}

	/**
	 * Add the names of all tables referenced in a SELECT statement to a list.
	 * 
	 * @param table
	 *            Parsed SelectStatement.
	 * @param tables
	 *            List of AliasedName objects to add to.
	 */
	private void addTablesFromSelect(List<TableNameAndAlias> tables) {
		// Get tables referenced in the FROM clause
		for (TableReference fromItem : m_from) {
			addTablesFromTableReference(fromItem, tables);
		}

		// Get Tables referenced in the WHERE clause (e.g. in an embedded
		// SELECT)
		addTablesFromExpression(m_whereClause, tables);

		// Get Tables referenced in GROUP BY...HAVING clause
		if (m_groupBy != null) {
			for (ExpressionOrConstant groupByItem : m_groupBy.getGroupBy()) {
				addTablesFromExpression(groupByItem, tables);
			}

			addTablesFromExpression(m_groupBy.getHaving(), tables);
		}

		// Get Tables referenced in ORDER BY clause
		if (m_orderBy != null) {
			for (OrderBy orderBy : m_orderBy) {
				addTablesFromExpression(orderBy.getExpression(), tables);
			}
		}

		// Get Tables referenced in SELECT ITEM clause
		for (SelectItem selectItem : m_select) {
			addTablesFromExpression(selectItem.getExpression(), tables);
		}
	}

	/**
	 * Add the names of all tables referenced in an SQL table reference to a list.
	 * 
	 * @param table
	 *            Parsed SQL TableReference.
	 * @param tables
	 *            List of AliasedName objects to add to.
	 */
	private void addTablesFromTableReference(TableReference table, List<TableNameAndAlias> tables) {
		if (table.isJoin()) {
			JoinedTable joinedTable = table.getJoinedTable();
			addTablesFromTableReference(joinedTable.getTableRef1(), tables);
			addTablesFromTableReference(joinedTable.getTableRef2(), tables);

			// FIXME what about JoinSpecification?
		} else {
			tables.add(table.getTable());
		}
	}

	/**
	 * Get the names of all tables referenced in general SQL expression.
	 * 
	 * @param expression
	 *            General parsed SQL expression..
	 * @param tables
	 *            List of AliasedName objects to add to.
	 */
	private void addTablesFromExpression(ExpressionOrConstant expression, List<TableNameAndAlias> tables) {
		if (expression instanceof Expression) {
			for (ExpressionOrConstant op : ((Expression) expression).getOperands()) {
				addTablesFromExpression(op, tables);
			}
		}
	}

	public void checkQueryIsODPSimple() throws RGMAPermanentException {
		checkQueryIsSimple();
		for (SelectItem item : m_select) {
			if (item.isExpression()) {
				ExpressionOrConstant esc = item.getExpression();
				if (!(esc instanceof Constant)) {
					throw new RGMAPermanentException("Query contains items with expressions: " + item);
				}
			}
		}
	}

	public boolean isODPSimpleQuery() {
		try {
			checkQueryIsODPSimple();
		} catch (RGMAPermanentException e) {
			return false;
		}
		return true;
	}
}
