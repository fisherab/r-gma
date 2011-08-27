package org.glite.rgma.server.services.registry;

import java.util.LinkedList;
import java.util.List;

import org.glite.rgma.server.services.sql.Constant;
import org.glite.rgma.server.services.sql.ExpressionOrConstant;
import org.glite.rgma.server.services.sql.Expression;
import org.glite.rgma.server.services.sql.SelectItem;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.RGMAPermanentException;

/**
 * Represents the parsed predicate of a consumer this will only return columns in the form of Column
 * Operator Value for simple queries. See the spec for a description of what that means.
 * 
 * TODO Some of this code might make more use of the parser classes.
 */
class ConsumerPredicate {

	/** True if this query has a complex operator. */
	private boolean m_hasComplexOperator;

	private String m_predicate;

	/**
	 * List of predicate columns
	 */
	private final List<ColumnOperatorValue> m_predicateColumns = new LinkedList<ColumnOperatorValue>();

	private SelectStatement m_select;

	private ExpressionOrConstant m_where;

	/**
	 * Create a ConsumerPredicate from a string predicate( where clause of an sql statement)
	 * 
	 * @param predicate
	 *            the predicate of a sql statement
	 * @throws RGMAPermanentException
	 */
	ConsumerPredicate(String predicate) throws RGMAPermanentException {
		m_predicate = predicate;

		/*
		 * construct a dummy select so that the predicate can be parsed by the sql parser.
		 * 
		 * TODO This code should be modified to not have the WHERE keyword optional.
		 */
		StringBuffer p = new StringBuffer("select c from v.t ");
		if (!m_predicate.equals("") && m_predicate.toLowerCase().indexOf("where") == -1) {
			p.append("WHERE " + predicate);
		} else {
			p.append(predicate);
		}

		try {
			m_select = SelectStatement.parse(p.toString());
		} catch (ParseException e) {
			throw new RGMAPermanentException("Error parsing predicate", e);
		}
		m_where = m_select.getWhere();
		parsePredicateColumns((Expression) m_where);
	}

	@Override
	public String toString() {
		return m_predicate;
	}

	/**
	 * Check that the query is simple
	 * 
	 * @throws RegistryException
	 *             if the query is not a simple query
	 */
	void checkQueryIsSimple() throws RGMAPermanentException {
		// no joins
		if (m_select.getFrom().size() > 1) {
			throw new RGMAPermanentException("Joins are not supported");
		}

		// has a distinct clause
		if (m_select.isDistinct()) {
			throw new RGMAPermanentException("IS DISTINCT is not supported");
		}

		// no GROUP BY
		if (m_select.getGroupBy() != null) {
			throw new RGMAPermanentException("GROUP BY is not supported");
		}

		// no ORDER BY
		if (m_select.getOrderBy() != null) {
			throw new RGMAPermanentException("ORDER BY is not supported");
		}

		// no ORs, etc allowed.
		if (m_hasComplexOperator) {
			throw new RGMAPermanentException("Predicate may only contain the operators '=', '>', '>=', '<', '<=', '<>', 'LIKE' and 'AND' in continuous queries");
		}

		for (SelectItem item : m_select.getSelect()) {
			// LOG.debug(item.toString() + ", " + item.getAggregate());
			if (item.toString().toUpperCase().startsWith("AVG(") || item.toString().toUpperCase().startsWith("MIN(") || item.toString().toUpperCase().startsWith("MAX(")
					|| item.toString().toUpperCase().startsWith("COUNT(") || item.toString().toUpperCase().startsWith("SUM(")) {
				throw new RGMAPermanentException("AVG, MIN, MAX, COUNT and SUM are notsupported");
			}
		}
	}

	/**
	 * returns a list of columns representing the predicate
	 * 
	 * @return a list of columns as ColumnOperatorValue object
	 */
	List<ColumnOperatorValue> getPredicateColumns() {
		return m_predicateColumns;
	}

	/**
	 * Adds the name, value and operator for the given fixed column to the predicate colum lis
	 * 
	 * @param where
	 *            Expression of the form "operand0" "operator" "operand1".
	 */
	private void addPredicateColumn(Expression where) {
		ColumnOperatorValue col = new ColumnOperatorValue(((Constant) where.getOperand(0)).getValue(), where.getOperator(), ((Constant) where.getOperand(1)).toString());
		m_predicateColumns.add(col);
	}

	/**
	 * Determines if an operator is simple.
	 * 
	 * @param operator
	 *            Operator to test.
	 * 
	 * @return true if the operator is simple.
	 */
	private boolean isSimpleOperator(String operator) {
		if (operator.equals("<>") || operator.equals("=") || operator.equals(">") || operator.equals("<") || operator.equals(">=") || operator.equals("<=")
				|| operator.equalsIgnoreCase("LIKE")) {
			return true;
		}

		return false;
	}

	/**
	 * Traverses the WHERE clause and any subqueries and stores fixed column details. Also sets
	 * <code>hasComplexOperator</code> flag.
	 * 
	 * @param where
	 *            WHERE clause.
	 */
	private void parsePredicateColumns(Expression where) {
		if (where != null) {
			String operator = where.getOperator();
			if (isSimpleOperator(operator)) {
				// if it's a simple operator, like "=" or ">"
				addPredicateColumn(where);
			} else if (!operator.toUpperCase().equals("AND")) {
				// it has a complex operator, like "OR", "IN", ...
				m_hasComplexOperator = true;
			} else {
				// must be an "AND"
				// divide it by 2 as there are there are 2 operands divided by an operator
				for (int i = 0; i < where.nbOperands(); i++) {
					try {
						Expression operand = (Expression) where.getOperand(i);

						if (isSimpleOperator(operand.getOperator())) {
							addPredicateColumn(operand);
						} else {
							parsePredicateColumns(operand);
						}
					} catch (ClassCastException e) {
						m_hasComplexOperator = true;
					}
				}
			}
		}
	}

}
