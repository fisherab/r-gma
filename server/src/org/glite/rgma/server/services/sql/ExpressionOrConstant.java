/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

/**
 * A common interface for all SQL Expressions and Constants.
 */
public abstract class ExpressionOrConstant {
    public static ExpressionOrConstant clone(ExpressionOrConstant e) {
        if (e == null) {
            return null;
        }
        if (e instanceof Constant) {
            return new Constant((Constant)e);
        } else { // expression
            return new Expression((Expression)e);
        } 
    }
}
