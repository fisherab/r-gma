/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.sql;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.glite.rgma.server.services.sql.Constant.Type;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.services.sql.parser.Parser;
import org.glite.rgma.server.services.sql.parser.TokenMgrError;

/**
 * A predicate of the form "WHERE column = constant AND column = constant AND ..." used by
 * producers.
 */
public class ProducerPredicate {
	private static final Map<DataType.Type, Constant.Type> typeToType = new HashMap<DataType.Type, Constant.Type>();

	static {
		typeToType.put(DataType.Type.CHAR, Constant.Type.STRING);
		typeToType.put(DataType.Type.VARCHAR, Constant.Type.STRING);
		typeToType.put(DataType.Type.DATE, Constant.Type.STRING);
		typeToType.put(DataType.Type.TIME, Constant.Type.STRING);
		typeToType.put(DataType.Type.TIMESTAMP, Constant.Type.STRING);
		typeToType.put(DataType.Type.INTEGER, Constant.Type.NUMBER);
		typeToType.put(DataType.Type.REAL, Constant.Type.NUMBER);
		typeToType.put(DataType.Type.DOUBLE_PRECISION, Constant.Type.NUMBER);
	}

	/**
	 * Parses the producer predicate into a ProducerPredicate object.
	 * 
	 * @param producerPredicate
	 *            Producer predicate as a String.
	 * 
	 * @return Producer predicate as a ProducerPredicate.
	 * 
	 * @throws ParseException
	 *             Thrown if the predicate is invalid.
	 */
	public static ProducerPredicate parse(String producerPredicate) throws ParseException {
		if (producerPredicate.trim().equals("")) {
			return new ProducerPredicate();
		}

		Parser parser = new Parser(new ByteArrayInputStream(producerPredicate.getBytes()));
		ProducerPredicate predicate;
		try {
			predicate = parser.ProducerPredicate();
			String s = parser.getNextToken().toString();
			if (s.length() != 0) {
				throw new ParseException("Extra token after valid producer predicate '" + s + "'");
			}
		} catch (TokenMgrError e) {
			throw new ParseException(e.getMessage());
		}

		return predicate;
	}

	/** List of column name/value pairs (can be empty). */
	private List<ColumnValue> m_columnValues;

	/**
	 * Creates a new ProducerPredicate with no column names/values.
	 */
	public ProducerPredicate() {
		m_columnValues = new ArrayList<ColumnValue>();
	}

	/**
	 * Creates a new ProducerPredicate with the given column names and values.
	 * 
	 * @param columnNameValues
	 *            List of column name/value pairs (can be empty).
	 */
	public ProducerPredicate(List<ColumnValue> columnNameValues) {
		m_columnValues = columnNameValues;
	}

	/**
	 * Gets the column names and values.
	 * 
	 * @return The column names and values.
	 */
	public List<ColumnValue> getColumnValues() {
		return m_columnValues;
	}

	/**
	 * Compares predicate column name with create table statement columns, return true if matches
	 * else returns false.
	 * 
	 * @param createTableStatement
	 * @return true/false
	 */
	public boolean isConsistent(CreateTableStatement createTableStatement) {
		List<ColumnDefinition> ctsColumns = createTableStatement.getColumns();
		Map<String, Constant.Type> ctsColumnNames = new HashMap<String, Constant.Type>();
		for (ColumnDefinition ctsColumn : ctsColumns) {
			ctsColumnNames.put(ctsColumn.getName().toUpperCase(), typeToType.get(ctsColumn.getType().getType()));
		}
		for (ColumnValue columnValue : m_columnValues) {
			String columnName = columnValue.getName().toUpperCase();
			Type type = ctsColumnNames.get(columnName);
			if (type != columnValue.getValue().getType()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (m_columnValues.size() == 0) {
			return "";
		}

		StringBuffer buf = new StringBuffer("WHERE ");
		boolean first = true;

		for (ColumnValue value : m_columnValues) {
			if (!first) {
				buf.append(" AND ");
			} else {
				first = false;
			}

			buf.append(value);
		}

		return buf.toString();
	}
}
