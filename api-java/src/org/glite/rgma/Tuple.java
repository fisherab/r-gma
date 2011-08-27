package org.glite.rgma;

/**
 * Represents a tuple (or row of a table). The size() method will return the number of columns in the tuple and the other
 * operations take a columnOffset parameter which must be in the range 0 to size-1.
 */
public class Tuple {

	private String[] m_data;

	Tuple(String[] data) {
		m_data = data;
	}

	/**
	 * Returns the boolean representation of the specified column.
	 * 
	 * @param columnOffset
	 *            offset of the column within the tuple
	 * @return <code>true</code> if the internal representation is equal, ignoring case, to the string "true". Nulls will be returned
	 *         as <code>false</code>.
	 * @throws RGMAPermanentException
	 *             if the offset is invalid
	 * @see #isNull
	 */
	public boolean getBoolean(int columnOffset) throws RGMAPermanentException {
		String s = get(columnOffset);
		return s == null ? false : Boolean.parseBoolean(s);
	}

	/**
	 * Returns the double representation of the specified column.
	 * 
	 * @param columnOffset
	 *            offset of the column within the tuple
	 * @return Double representation of the column. Nulls will be returned as 0.
	 * @throws RGMAPermanentException
	 *             if the value cannot be represented as a double or the offset is invalid
	 * @see #isNull
	 */
	public double getDouble(int columnOffset) throws RGMAPermanentException {
		String s = get(columnOffset);
		try {
			return s == null ? 0d : Double.parseDouble(s);
		} catch (NumberFormatException e) {
			throw new RGMAPermanentException("Value '" + s + "' does not represent a Double");
		}
	}

	/**
	 * Returns the float representation of the specified column.
	 * 
	 * @param columnOffset
	 *            offset of the column within the tuple
	 * @return Float representation of the column. Nulls will be returned as 0.
	 * @throws RGMAPermanentException
	 * @throws RGMAPermanentException
	 *             if the value cannot be represented as a float or the offset is invalid
	 * @see #isNull
	 */
	public float getFloat(int columnOffset) throws RGMAPermanentException {
		String s = get(columnOffset);
		try {
			return s == null ? 0f : Float.parseFloat(s);
		} catch (NumberFormatException e) {
			throw new RGMAPermanentException("Value '" + s + "' does not represent a Float");
		}
	}

	/**
	 * Returns the integer representation of the specified column.
	 * 
	 * @param columnOffset
	 *            offset of the column within the tuple
	 * @return integer representation of the column. Nulls will be returned as 0.
	 * @throws RGMAPermanentException
	 *             if the value cannot be represented as an integer or the offset is invalid
	 * @see #isNull
	 */
	public int getInt(int columnOffset) throws RGMAPermanentException {
		String s = get(columnOffset);
		try {
			return s == null ? 0 : Integer.parseInt(s);
		} catch (NumberFormatException e) {
			throw new RGMAPermanentException("Value '" + s + "' does not represent a Integer");
		}
	}

	/**
	 * Returns the string representation of the specified column.
	 * 
	 * @param columnOffset
	 *            offset of the column within the tuple
	 * @return string representation of the column. An empty string will be returned for nulls.
	 * @throws RGMAPermanentException
	 *             if the offset is invalid
	 * @see #isNull
	 */
	public String getString(int columnOffset) throws RGMAPermanentException {
		String s = get(columnOffset);
		return s == null ? "" : s;
	}

	/**
	 * Returns the null status of the specified column.
	 * 
	 * @param columnOffset
	 *            offset of the column within the tuple
	 * @return <code>true</code> if the column holds a null value otherwise <code>false</code>.
	 * @throws RGMAPermanentException
	 *             if the offset is invalid
	 */
	public boolean isNull(int columnOffset) throws RGMAPermanentException {
		return get(columnOffset) == null;
	}

	private String get(int columnOffset) throws RGMAPermanentException {
		if (columnOffset < 0 || columnOffset >= m_data.length) {
			throw new RGMAPermanentException("column offset must be between 0 and " + (m_data.length - 1));
		}
		return m_data[columnOffset];
	}

}
