/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import org.glite.rgma.server.services.sql.parser.ParseException;

/**
 * Represents an SQL constant.
 */
public class Constant extends ExpressionOrConstant {
    /** Value of constant. */
    private String m_value;

    /** Constant type, initially UNKNOWN. */
    private Type m_type = Type.UNKNOWN;

    /**
     * Create a new constant, given its name and type.
     *
     * @param value Constant's value
     * @param type Constant's type
     */
    public Constant(String value, Type type) {
        m_value = value;
        m_type = type;
    }
    
    /**
     * Creates a new Constant with the same attributes as <code>c</code>.
     * 
     * @param c Constant to copy.
     */
    public Constant(Constant c) {
        m_value = c.m_value;
        m_type = c.m_type;
    }

    /**
     * Returns the constant type.
     *
     * @return The constant type
     */
    public Type getType() {
        return m_type;
    }

    /**
     * Returns the constant value.
     *
     * @return The constant value
     */
    public String getValue() {
        return m_value;
    }

    /**
     * Returns String representation of Constant in a form which SQL can use.
     *
     * @return String representation of Constant.  Either its value, or if it is
     *         a STRING, the value with any quotes escaped and the whole string
     *         surrounded by quotes.
     */
    public String toString() {
        if (m_type == Type.NULL) {
            return "NULL";
        } else if (m_type == Type.STRING) {        
            return '\'' + m_value.replace("'", "''") + '\'';
        } else {
            return m_value;
        }
    }

    /**
     * Gets the name of the type of this Constant.
     *
     * @return The name of the type with the size in characters if it
     * is of type STRING.
     */
    public String getTypeName() {
        if (m_type == Type.STRING) {
            return "STRING(" + m_value.length() + ")";
        } else if (m_type == Type.NUMBER) {
            return "NUMBER";
        } else if (m_type == Type.NULL) {
            return "NULL";
        } else {
            return "UNKNOWN";
        }
    }

    /**
     * Determines if this Constant represents NULL.
     *
     * @return <code>true</code> if this Constant represents NULL.
     */
    public boolean isNull() {
        return (getValue() == null) || (getType() == Type.NULL);
    }
    
    public void prependTableName(String t) throws ParseException {
    	if (m_value.contains(".")) {
    		throw new ParseException("Cannot prepend to column names with dots: " + m_value);
    	}
    	m_value = t + "." + m_value;
    }

    /** Constant type. */
    public enum Type {UNKNOWN, 
        COLUMN_NAME, 
        NULL, 
        NUMBER, 
        STRING;
    }

	public void setValue(String value) {
		m_value = value;	
	}
}
