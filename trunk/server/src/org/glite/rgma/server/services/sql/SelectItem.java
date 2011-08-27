/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import org.glite.rgma.server.services.sql.Constant.Type;

/**
 * SelectItem: an item in the SELECT part of an SQL query. (The SELECT part of a query is a List of SelectItem objects).
 */
public class SelectItem {

	private String m_alias;

	/** Expression if more complex than just aliased name. */
	private ExpressionOrConstant m_expression;

	private ColumnName m_columnName;

	/** Is it a COUNT DISTINCT item. */
	private boolean m_countDistinct;

	private String m_text;

	/**
	 * Creates a new SELECT item, given its text. This may be a column name or "COUNT(*)"
	 */
	public SelectItem(String text) {
		m_text = text;
	}

	/**
	 * If this item is an aggregate function, return the function name.
	 * 
	 * @return The name of an aggregate function (generally SUM, AVG, MAX, MIN), or null if there's no aggregate.
	 *         Example: SELECT name, AVG(age) FROM people; -> null for the "name" item, and "AVG" for the "AVG(age)"
	 *         item.
	 */
	public String getAggregate() {
		if (m_text != null && m_text.equals("COUNT(*)")) {
			return "COUNT";
		} else if (m_expression != null && m_expression instanceof Expression) {
			return ((Expression) m_expression).getAggregate();
		} else {
			return null;
		}
	}

	public SelectItem() {}

	public SelectItem(SelectItem other) {
		m_alias = other.m_alias;
		m_expression = ExpressionOrConstant.clone(other.m_expression);
		m_columnName = other.m_columnName;
		m_countDistinct = other.m_countDistinct;
		m_text = other.m_text;
	}

	public boolean isCountDistinct() {
		return m_countDistinct;
	}

	public void setCountDistinct(boolean countDistinct) {
		m_countDistinct = countDistinct;
	}

	/**
	 * Initialize this SELECT item as an SQL expression (not a column name).
	 */
	public void setExpression(ExpressionOrConstant e) {
		m_expression = e;
	}

	/**
	 * Determines if this select item is an expression. Example: SELECT a+b, c FROM num; a+b is an expression, c is not.
	 * 
	 * @return true if this item is an SQL expression, false if not.
	 */
	public boolean isExpression() {
		return (m_expression != null);
	}

	public void setAlias(String alias) {
		m_alias = alias;
	}

	public String getAlias() {
		return m_alias;
	}

	public ExpressionOrConstant getExpression() {
		if (isExpression()) {
			return m_expression;
		} else if (m_columnName != null) {
			return new Constant(m_columnName.getColumnName(), Type.COLUMN_NAME);
		} else {
			return new Constant(m_text, Type.COLUMN_NAME); 
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (m_columnName != null) {
			sb.append(m_columnName);
		} else if (m_text != null) {
			sb.append(m_text);
		} else if (m_expression != null) {
			sb.append(m_expression);
		}
		if (m_alias != null) {
			sb.append(" AS ").append(m_alias);
		}
		return sb.toString();

	}
}
