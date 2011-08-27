package org.glite.rgma.server.services.sql;

import java.io.ByteArrayInputStream;

import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.sql.parser.Parser;
import org.glite.rgma.server.services.sql.parser.TokenMgrError;

/*
 * Represents a "normal" WHERE clause
 * 
 * The following, though legal SQL are not accepted:
 * 
 *     	"where (select count(*) from S)",
 *   	"WHERE true"
 */
public class WhereClause {

	private Expression m_expression;

	WhereClause(Expression expression) {
		m_expression = expression;
	}

	/**
	 * Parses the WHERE clause into an object.
	 * 
	 * @param selectStatement
	 *            SQL WHERE Clause as a String.
	 * 
	 * @return SQL WHERE Clause as a WhereClause.
	 * 
	 * @throws ParseException
	 *             Thrown if the input clause is invalid.
	 */
	public static WhereClause parse(String whereClause) throws ParseException {
		Parser parser = new Parser(new ByteArrayInputStream((whereClause).getBytes()));
		Expression expression;
		ExpressionOrConstant expSelConst = null;
		try {
			expSelConst = parser.WhereClause();
			expression = (Expression) expSelConst;
			String s = parser.getNextToken().toString();
			if (s.length() != 0) {
				throw new ParseException("Extra token after valid WHERE clause '" + s + "'");
			}
		} catch (TokenMgrError e) {
			throw new ParseException(e.getMessage());
		} catch (ClassCastException e) {
			if (expSelConst instanceof Constant) {
				throw new ParseException("WHERE clause with just a constant is not accepted");
			} else { // SelectStatement
				throw new ParseException("WHERE clause with just a select statement is not accepted");
			}
		}
		return new WhereClause(expression);
	}
	
	public String toString() {
		return "WHERE " + m_expression; 
	}
	
	public Expression getExpression() {
		return m_expression;
	}
	
	public void prependTableName(String t) throws ParseException {
		m_expression.prependTableName(t);
	}
}
