/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import java.sql.SQLException;
import java.sql.Types;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.sql.DataType;
import org.glite.rgma.server.services.sql.DataType.Type;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.TupleSetWithLastTUID;

/**
 * Common methods for implementation of TupleStoreDatabase.
 */
public class TupleStoreDatabaseBase {
	/** Prefix for all temporary tables. */
	protected static final String CURSOR_TABLE_PREFIX = "CursorTable";

	/** Reference to logging utility. */
	protected static final Logger LOG = Logger.getLogger(TupleStoreConstants.TUPLE_STORE_LOGGER);

	private static final String RGMA_TUID_COLUMN_NAME = ReservedColumns.RGMA_TUID_COLUMN_NAME.toUpperCase();

	private static final String RGMA_TUID_ONE_OFF_COLUMN_NAME = ReservedColumns.RGMA_TUID_ONE_OFF_COLUMN_NAME.toUpperCase();

	private static final String RGMA_INSERT_TIME_COLUMN_NAME = ReservedColumns.RGMA_INSERT_TIME_COLUMN_NAME.toUpperCase();

	/**
	 * Converts a JDBC ResultSet to an R-GMA ResultSet.
	 * 
	 * @param jdbcResultSet
	 *            ResultSet to convert.
	 * @return R-GMA ResultSet containing all data and metadata from jdbcResultSet.
	 * @throws SQLException
	 *             If the conversion fails.
	 * @throws RGMAPermanentException
	 */
	protected static TupleSetWithLastTUID convertResultSet(java.sql.ResultSet jdbcResultSet) throws SQLException, RGMAPermanentException {
		java.sql.ResultSetMetaData jdbcMetaData = jdbcResultSet.getMetaData();
		int columnCount = jdbcMetaData.getColumnCount();

		int insertTimeColumnNumber = 0;
		int tuidColumnNumber = 0;
		int tuidOneOffColumnNumber = 0;
		int newCount = 0;
		for (int c = 1; c <= columnCount; c++) {
			String columnName = jdbcMetaData.getColumnName(c);
			if (columnName.toUpperCase().endsWith(RGMA_TUID_COLUMN_NAME)) {
				tuidColumnNumber = c;
			} else if (columnName.toUpperCase().endsWith(RGMA_INSERT_TIME_COLUMN_NAME)) {
				insertTimeColumnNumber = c;
			} else if (columnName.toUpperCase().endsWith(RGMA_TUID_ONE_OFF_COLUMN_NAME)) {
				tuidOneOffColumnNumber = c;
			} else {
				newCount++;
			}
		}

		TupleSet resultSet = new TupleSet();
		int lastTUID = 0;
		while (jdbcResultSet.next()) {
			String[] row = new String[newCount];
			int i = 0;
			for (int c = 1; c <= columnCount; c++) {
				if (c == tuidColumnNumber) {
					lastTUID = jdbcResultSet.getInt(c);
				} else if (c != insertTimeColumnNumber && c != tuidOneOffColumnNumber) {
					row[i] = jdbcResultSet.getString(c);
					if (jdbcResultSet.wasNull()) {
						row[i] = null;
					}
					i++;
				}
			}
			resultSet.addRow(row);
		}
		return new TupleSetWithLastTUID(resultSet, lastTUID);
	}

	/**
	 * Retrieves the size of the given type, e.g. returns 5 from "VARCHAR(5)". TODO get rid of this fn
	 * 
	 * @param typeStr
	 *            Type as a String.
	 * @return Size of type, or 0 if it is not specified.
	 */
	protected static int getSizeFromType(String typeStr) {
		int leftBracket = typeStr.indexOf('(');
		int rightBracket = typeStr.indexOf(')');

		if (leftBracket == -1 || rightBracket == -1) {
			return 0;
		}

		String sizeStr = typeStr.substring(leftBracket + 1, rightBracket);
		int size = 0;

		try {
			size = Integer.parseInt(sizeStr);
		} catch (NumberFormatException e) {
			// return RGMAPermanentException("Invalid type length");
		}

		return size;
	}

	/**
	 * Translates a type integer (from java.sql.Types) into a DataType.
	 * 
	 * @param columnType
	 *            Type as an integer.
	 * @return Data type representing given type.
	 */
	protected static DataType translateTypeInt(int columnType) {
		switch (columnType) {
		case Types.CHAR:
			return new DataType(Type.CHAR, 0);

		case Types.DATE:
			return new DataType(Type.DATE, 0);

		case Types.DOUBLE:
			return new DataType(Type.DOUBLE_PRECISION, 0);

		case Types.FLOAT:
			return new DataType(Type.REAL, 0);

		case Types.INTEGER:
			return new DataType(Type.INTEGER, 0);

		case Types.REAL:
			return new DataType(Type.REAL, 0);

		case Types.TIME:
			return new DataType(Type.TIME, 0);

		case Types.TIMESTAMP:
			return new DataType(Type.TIMESTAMP, 0);

		default:
			return new DataType(Type.VARCHAR, 0);
		}
	}

	/** Current cursor ID (this will be allocated on next call to nextCursorID()). */
	protected int m_currentCursorID;

	/**
	 * Returns the next cursor ID to use and increments it.
	 * 
	 * @return Next unique cursor ID.
	 */
	protected synchronized int nextCursorID() {
		return m_currentCursorID++;
	}
}