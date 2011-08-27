/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.sql.SQLException;
import java.util.List;

import org.glite.rgma.server.services.sql.Tuple.UnknownAttribute;

/**
 * Evaluate SQL expressions
 */
public class SQLExpEvaluator {
    @SuppressWarnings("serial")
	public class NullFound extends Exception {
	}

	/**
     * Evaluate a boolean expression to true or false (for example, SQL WHERE
     * clauses are boolean expressions)
     *
     * @param tuple The tuple on which to evaluate the expression
     * @param exp The expression to evaluate
     *
     * @return true if the expression evaluate to true for this tuple, false if
     *         not.
     *
     * @throws SQLException DOCUMENT ME!
	 * @throws NullFound 
	 * @throws UnknownAttribute 
     */
    public boolean eval(Tuple tuple, ExpressionOrConstant exp) throws SQLException, NullFound, UnknownAttribute {
        if ((tuple == null) || (exp == null)) {
            throw new SQLException("SQLExpEvaluator.eval(): null argument or operator");
        }

        if (!(exp instanceof Expression)) {
            throw new SQLException("SQLExpEvaluator.eval(): only expressions are supported");
        }

        Expression pred = (Expression) exp;
        String op = pred.getOperator();

        if (op.equals("AND")) {
            boolean and = true;

            for (int i = 0; i < pred.nbOperands(); i++) {
                and &= eval(tuple, pred.getOperand(i));
            }

            return and;
        } else if (op.equals("OR")) {
            boolean or = false;

            for (int i = 0; i < pred.nbOperands(); i++) {
                or |= eval(tuple, pred.getOperand(i));
            }

            return or;
        } else if (op.equals("NOT")) {
            return !eval(tuple, pred.getOperand(0));
        } else if (op.equals("=")) {
            return evalCmp(tuple, pred.getOperands()) == 0;
        } else if (op.equals("!=")) {
            return evalCmp(tuple, pred.getOperands()) != 0;
        } else if (op.equals("<>")) {
            return evalCmp(tuple, pred.getOperands()) != 0;
        } else if (op.equals("#")) {
            throw new SQLException("SQLExpEvaluator.eval(): Operator # not supported");
        } else if (op.equals(">")) {
            return evalCmp(tuple, pred.getOperands()) > 0;
        } else if (op.equals(">=")) {
            return evalCmp(tuple, pred.getOperands()) >= 0;
        } else if (op.equals("<")) {
            return evalCmp(tuple, pred.getOperands()) < 0;
        } else if (op.equals("<=")) {
            return evalCmp(tuple, pred.getOperands()) <= 0;
        } else if (op.equals("BETWEEN") || op.equals("NOT BETWEEN")) {
            // Between: borders included
            Expression newexp = new Expression("AND", new Expression(">=", pred.getOperand(0), pred.getOperand(1)),
                    new Expression("<=", pred.getOperand(0), pred.getOperand(2)));

            if (op.equals("NOT BETWEEN")) {
                return !eval(tuple, newexp);
            } else {
                return eval(tuple, newexp);
            }
        } else if (op.equals("LIKE") || op.equals("NOT LIKE")) {
            throw new SQLException("SQLExpEvaluator.eval(): Operator (NOT) LIKE not supported");
        } else if (op.equals("IN") || op.equals("NOT IN")) {
            Expression newexp = new Expression("OR");

            for (int i = 1; i < pred.nbOperands(); i++) {
                newexp.addOperand(new Expression("=", pred.getOperand(0), pred.getOperand(i)));
            }

            if (op.equals("NOT IN")) {
                return !eval(tuple, newexp);
            } else {
                return eval(tuple, newexp);
            }
        } else if (op.equals("IS NULL")) {
        	throw new NullFound ();
 
/* TODO this code and the block below should be fixed to support nulls */        	
//            if ((pred.nbOperands() <= 0) || (pred.getOperand(0) == null)) {
//                return true;
//            }
//
//            ExpSelConst x = pred.getOperand(0);
//           
//            if (x instanceof Constant) {
//                return (((Constant) x).getType() == Constant.Type.NULL);
//            } else {
//                throw new SQLException("SQLExpEvaluator.eval(): can't eval IS (NOT) NULL");
//            }
        } else if (op.equals("IS NOT NULL")) {
        	throw new NullFound ();
//            Expression x = new Expression("IS NULL");
//            x.setOperands(pred.getOperands());
//
//            return !eval(tuple, x);
        } else {
            throw new SQLException("SQLExpEvaluator.eval(): Unknown operator " + op);
        }
    }

    /**
     * Evaluate a numeric or string expression (example: a+1)
     *
     * @param tuple The tuple on which to evaluate the expression
     * @param exp The expression to evaluate
     *
     * @return The expression's value
     *
     * @throws SQLException DOCUMENT ME!
     * @throws UnknownAttribute 
     */
    private Object evalExpValue(Tuple tuple, ExpressionOrConstant exp)
        throws SQLException, UnknownAttribute {
        Object result = null;

        if (exp instanceof Constant) {
            Constant c = (Constant) exp;
            
            switch (c.getType()) {
            case COLUMN_NAME:
                result = tuple.getAttValue(c.getValue());

                break;

            case NUMBER:
                result = new Double(c.getValue());

                break;

            case STRING:default:
                result = c.getValue();

                break;
            }
        } else if (exp instanceof Expression) {
            result = new Double(evalNumericExp(tuple, (Expression) exp));
        }

        return result;
    }

    private double evalCmp(Tuple tuple, List<ExpressionOrConstant> operands) throws SQLException, UnknownAttribute {
        if (operands.size() < 2) {
            throw new SQLException("SQLExpEvaluator.evalCmp(): Trying to compare less than two values");
        }

        if (operands.size() > 2) {
            throw new SQLException("SQLExpEvaluator.evalCmp(): Trying to compare more than two values");
        }

        Object operand1 = evalExpValue(tuple, (ExpressionOrConstant) operands.get(0));
        Object operand2 = evalExpValue(tuple, (ExpressionOrConstant) operands.get(1));

        if (operand1 instanceof String || operand2 instanceof String) {
            return (operand1.equals(operand2) ? 0 : (-1));
        }

        if (operand1 instanceof Number && operand2 instanceof Number) {
            return ((Number) operand1).doubleValue() - ((Number) operand2).doubleValue();
        } else {
            throw new SQLException("SQLExpEvaluator.evalCmp(): can't compare (" + operand1 + ") with (" +
                operand2 + ")");
        }
    }

    private double evalNumericExp(Tuple tuple, Expression exp)
        throws SQLException, UnknownAttribute {
        if ((tuple == null) || (exp == null) || (exp.getOperator() == null)) {
            throw new SQLException("SQLExpEvaluator.eval(): null argument or operator");
        }

        String op = exp.getOperator();
        Object o1 = evalExpValue(tuple, (ExpressionOrConstant) exp.getOperand(0));

        if (!((o1 instanceof Double) || (o1 instanceof Integer))) {
            throw new SQLException("SQLExpEvaluator.evalNumericExp(): expression not numeric");
        }

        double dobj = ((Number) o1).doubleValue();

        if (op.equals("+")) {
            double val = dobj;

            for (int i = 1; i < exp.nbOperands(); i++) {
                Object obj = evalExpValue(tuple, (ExpressionOrConstant) exp.getOperand(i));
                val += ((Number) obj).doubleValue();
            }

            return val;
        } else if (op.equals("-")) {
            double val = dobj;

            if (exp.nbOperands() == 1) {
                return -val;
            }

            for (int i = 1; i < exp.nbOperands(); i++) {
                Object obj = evalExpValue(tuple, (ExpressionOrConstant) exp.getOperand(i));
                val -= ((Number) obj).doubleValue();
            }

            return val;
        } else if (op.equals("*")) {
            double val = dobj;

            for (int i = 1; i < exp.nbOperands(); i++) {
                Object obj = evalExpValue(tuple, (ExpressionOrConstant) exp.getOperand(i));
                val *= ((Number) obj).doubleValue();
            }

            return val;
        } else if (op.equals("/")) {
            double val = dobj;

            for (int i = 1; i < exp.nbOperands(); i++) {
                Object obj = evalExpValue(tuple, (ExpressionOrConstant) exp.getOperand(i));
                val /= ((Number) obj).doubleValue();
            }

            return val;
        } else if (op.equals("**")) {
            double val = dobj;

            for (int i = 1; i < exp.nbOperands(); i++) {
                Object obj = evalExpValue(tuple, (ExpressionOrConstant) exp.getOperand(i));
                val = Math.pow(val, ((Number) obj).doubleValue());
            }

            return val;
        } else {
            throw new SQLException("SQLExpEvaluator.evalNumericExp(): Unknown operator " + op);
        }
    }
}
