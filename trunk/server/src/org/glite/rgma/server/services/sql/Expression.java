/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import org.glite.rgma.server.services.sql.Constant.Type;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.RGMAPermanentException;

import java.util.List;
import java.util.Vector;

/**
 * An SQL expression: an operator and one or more operands.
 */
public class Expression extends ExpressionOrConstant {
	/**
	 * Determines if the operator is an aggregation operator (SUM, AVG, MAX, MIN or COUNT).
	 * 
	 * @param operator
	 *            Operator.
	 * @return <code>true</code> if the operator is an aggregation.
	 */
	private static boolean isAggregate(String operator) {
		operator = operator.toUpperCase();
		return operator.equals("SUM") || operator.equals("AVG") || operator.equals("MAX") || operator.equals("MIN") || operator.equals("COUNT");
	}

	/**
	 * Determines if the operator is a postfix operator.
	 * 
	 * @param operator
	 *            Operator.
	 * @return <code>true</code> if the operator is postfix.
	 */
	private static boolean isPostFix(String operator) {
		return operator.equals("IS NULL") || operator.equals("IS NOT NULL");
	}

	/**
	 * Determines if the operator needs parentheses.
	 * 
	 * @param operator
	 *            Operator.
	 * @return <code>true</code> if the operator needs parentheses.
	 */
	private static boolean needsParentheses(String operator) {
		operator = operator.toUpperCase();
		return !(operator.equals("ANY") || operator.equals("ALL") || operator.equals("UNION") || isAggregate(operator));
	}

	/** Operands, list of ExpSelConst objects. */
	private List<ExpressionOrConstant> m_operands;

	/** Operator. */
	private String m_operator;

	/**
	 * Creates a new Expression with the same attributes as <code>expression</code>.
	 * 
	 * @param expression
	 *            Expression to copy.
	 */
	public Expression(Expression expression) {
		m_operator = expression.m_operator;
		for (ExpressionOrConstant esc : expression.m_operands) {
			addOperand(ExpressionOrConstant.clone(esc));
		}
	}

	/**
	 * Creates an SQL Expression given the operator
	 * 
	 * @param operator
	 *            The operator
	 */
	public Expression(String operator) {
		m_operator = operator;
	}

	/**
	 * Creates an SQL Expression given the operator and 1st operand
	 * 
	 * @param operator
	 *            The operator
	 * @param operand1
	 *            The 1st operand
	 */
	public Expression(String operator, ExpressionOrConstant operand1) {
		m_operator = operator;
		addOperand(operand1);
	}

	/**
	 * Creates an SQL Expression given the operator, 1st and 2nd operands
	 * 
	 * @param operator
	 *            The operator
	 * @param operand1
	 *            The 1st operand
	 * @param operand2
	 *            The 2nd operand
	 */
	public Expression(String operator, ExpressionOrConstant operand1, ExpressionOrConstant operand2) {
		m_operator = operator;
		addOperand(operand1);
		addOperand(operand2);
	}

	/**
	 * Add san operand to the current expression.
	 * 
	 * @param operand
	 *            The operand to add.
	 */
	public void addOperand(ExpressionOrConstant operand) {
		if (m_operands == null) {
			m_operands = new Vector<ExpressionOrConstant>();
		}

		m_operands.add(operand);
	}

	/**
	 * Check that this expression is simple. A simple expression is either:
	 * <ul>
	 * <li>Of the form <code>column operator value</code> where the operator is one of =, &gt;, &lt;, &gt;= or &lt;=, OR
	 * </li>
	 * <li>Of the form <code>expression AND expression (AND expression...)</code> where all the expressions are simple
	 * </ul>
	 */
	public void checkExpressionIsSimple() throws RGMAPermanentException {
		if (m_operator.toUpperCase().equals("AND")) {
			for (ExpressionOrConstant op : m_operands) {
				if (op instanceof Expression) {
					((Expression) op).checkExpressionIsSimple();
				}
			}
		} else if (!isSimpleOperator()) {
			throw new RGMAPermanentException("Operator '" + m_operator + "' is not one of '=', '<>', '>', '<', '>=', '<=', 'LIKE' or 'AND'");
		} else {
			for (ExpressionOrConstant op : m_operands) {
				if (!(op instanceof Constant)) {
					throw new RGMAPermanentException("Expression operand is not constant: " + op);
				}
			}
		}
	}

	public String getAggregate() {
		String result = null;
		if (nbOperands() == 1) {
			ExpressionOrConstant operand = getOperand(0);
			if (operand instanceof Constant && isAggregate(m_operator)) {
				result = m_operator;
			}
		}
		return result;
	}

	/**
	 * Gets an operand according to its index (position).
	 * 
	 * @param pos
	 *            The operand index, starting at 0.
	 * @return The operand at the specified index, null if out of bounds.
	 */
	public ExpressionOrConstant getOperand(int pos) {
		if (m_operands == null || pos >= m_operands.size() || pos < 0) {
			return null;
		}

		return m_operands.get(pos);
	}

	/**
	 * Gets this expression's operands.
	 * 
	 * @return the operands (as a Vector of ExpSelConst objects).
	 */
	public List<ExpressionOrConstant> getOperands() {
		return m_operands;
	}

	/**
	 * Gets this expression's operator.
	 * 
	 * @return the operator.
	 */
	public String getOperator() {
		return m_operator;
	}

	/**
	 * Gets the number of operands
	 * 
	 * @return The number of operands
	 */
	public int nbOperands() {
		if (m_operands == null) {
			return 0;
		}

		return m_operands.size();
	}

	public void prependTableName(String t) throws ParseException {
		for (ExpressionOrConstant operand : m_operands) {
			if (operand instanceof Constant) {
				Constant constant = (Constant) operand;
				if (constant.getType() == Type.UNKNOWN || constant.getType() == Type.COLUMN_NAME) {
					constant.prependTableName(t);
				}

			} else if (operand instanceof Expression) {
				((Expression) operand).prependTableName(t);
			} else { // SelectStatement
				throw new ParseException("Unexpected select statement within WHERE clause");
			}
		}
	}

	/**
	 * Sets the operands list.
	 * 
	 * @param v
	 *            A vector that contains all operands (ExpSelConst objects).
	 */
	public void setOperands(List<ExpressionOrConstant> v) {
		m_operands = v;
	}

	/**
	 * Returns the expression in reverse polish notation.
	 * 
	 * @return The current expression in reverse polish notation.
	 */
	public String toReversePolish() {
		StringBuffer buf = new StringBuffer("(");
		buf.append(m_operator);

		for (int i = 0; i < nbOperands(); i++) {
			ExpressionOrConstant opr = getOperand(i);

			if (opr instanceof Expression) {
				buf.append(" " + ((Expression) opr).toReversePolish());
			} else {
				buf.append(" " + opr.toString());
			}
		}

		buf.append(")");

		return buf.toString();
	}

	/**
	 * Returns a String representation of this Expression.
	 * 
	 * @return A String representation of this Expression.
	 */
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();

		if (needsParentheses(m_operator)) {
			buf.append("(");
		}

		ExpressionOrConstant operand;

		switch (nbOperands()) {
		case 1:
			operand = getOperand(0);

			if (operand instanceof Constant) {
				// Operator may be an aggregate function (MAX, SUM...)
				if (isAggregate(m_operator)) {
					buf.append(m_operator + "(" + operand.toString() + ")");
				} else if (isPostFix(m_operator)) {
					buf.append(operand.toString() + " " + m_operator);
				} else {
					buf.append(m_operator + " " + operand.toString());
				}
			} else {
				buf.append(m_operator + " " + operand.toString());
			}

			break;

		case 3:

			if (m_operator.toUpperCase().endsWith("BETWEEN")) {
				buf.append(getOperand(0).toString() + " " + m_operator + " " + getOperand(1).toString() + " AND " + getOperand(2).toString());

				break;
			}

		default:

			boolean in_op = m_operator.equals("IN") || m_operator.equals("NOT IN");
			int nb = nbOperands();

			for (int i = 0; i < nb; i++) {
				if (in_op && i == 1) {
					buf.append(" " + m_operator + " (");
				}

				buf.append(getOperand(i).toString());
				if (i < nb - 1) {
					if (m_operator.equals(",") || in_op && i > 0) {
						buf.append(", ");
					} else if (!in_op) {
						buf.append(" " + m_operator + " ");
					}
				}
			}

			if (in_op) {
				buf.append(")");
			}

			break;
		}

		if (needsParentheses(m_operator)) {
			buf.append(")");
		}

		return buf.toString();
	}

	/**
	 * Determines if the operator is simple.
	 * 
	 * @return <code>true</code> if the operator is simple.
	 */
	private boolean isSimpleOperator() {
		if (m_operator.equals("=") || m_operator.equals("<>") || m_operator.equals(">") || m_operator.equals("<") || m_operator.equals(">=")
				|| m_operator.equals("<=") || m_operator.equals("LIKE")) {
			return true;
		}
		return false;
	}
}
