/* Copyright (c) 2001 EU DataGrid.
 * For license conditions see http://www.eu-datagrid.org/license.html
 */

package org.glite.rgma.server.system;

@SuppressWarnings("serial")
public class NumericException extends Exception {

	private int m_number;

	public NumericException(int number, String message) {
		super(message);
		m_number = number;
	}

	public int getNumber() {
		return m_number;
	}

}
