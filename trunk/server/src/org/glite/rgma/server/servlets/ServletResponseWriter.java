/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;

/**
 * Writes the output from an R-GMA request to the servlet response.
 */
public class ServletResponseWriter {

	private PrintWriter m_writer;

	/**
	 * Constructor
	 * 
	 * @throws IOException
	 */
	public ServletResponseWriter(HttpServletResponse response) throws IOException {
		response.setContentType("text/xml; charset=UTF-8");
		m_writer = response.getWriter();
	}

	/**
	 * This is primarily for testing out of the servlet environment
	 * 
	 * @param writer
	 */
	public ServletResponseWriter(PrintWriter writer) {
		m_writer = writer;
	}

	/**
	 * Close the response.
	 */
	public void close() throws IOException {
		m_writer.close();
	}

	public final void writeBoolean(boolean value) throws IOException {
		writeString(new Boolean(value).toString());
	}

	public void writeException(NumericException e) {
		m_writer.print("<i n=\"" + e.getNumber() + "\" m=\"" + normalize(e.getMessage()) + "\"/>");
	}

	public void writeException(RGMATemporaryException e) throws IOException {
		m_writer.print("<t o=\"" + e.getNumSuccessfulOps() + "\" m=\"" + normalize(e.getMessage()) + "\"/>");
	}

	public void writeException(RGMAPermanentException e) throws IOException {
		m_writer.print("<p o=\"" + e.getNumSuccessfulOps() + "\" m=\"" + normalize(e.getMessage()) + "\"/>");
	}

	public void writeException(UnknownResourceException e) throws IOException {
		m_writer.print("<u/>");
	}

	/**
	 * Write an int as an XMLResultSet
	 */
	public final void writeInt(int value) throws IOException {
		m_writer.print("<r><v>" + value + "</v><e/></r>");
	}

	/**
	 * Write a long as an XMLResultSet
	 */
	public final void writeLong(long value) throws IOException {
		m_writer.print("<r><v>" + value + "</v><e/></r>");
	}

	/**
	 * Write a ResultSet response
	 * 
	 * @throws IOException
	 */
	public void writeResultSet(TupleSet rs) throws IOException {
		writeOneResultSet(rs);
	}

	/**
	 * Write a list of ResultSets as XML
	 */
	public void writeResultSets(List<TupleSet> rss) throws IOException {
		m_writer.print("<s>");
		for (TupleSet rs : rss) {
			writeOneResultSet(rs);
		}
		m_writer.print("</s>");
	}

	/**
	 * Write an 'OK' status
	 */
	public void writeStatusOK() throws IOException {
		writeString("OK");
	}

	/**
	 * Write a String as an XMLResultSet For efficiency, we don't call writeXMLStringArray.
	 */
	public final void writeString(String value) throws IOException {
		m_writer.print("<r><v>" + value + "</v><e/></r>");
	}

	/**
	 * Write a String Array as an XMLResultSet
	 */
	public final void writeStringArray(String[] values) throws IOException {
		m_writer.print("<r>");
		for (String value : values) {
			m_writer.print("<v>" + normalize(value) + "</v>");
		}
		m_writer.print("<e/></r>");
	}

	/**
	 * Write raw XML
	 */
	public final void writeXML(String xmlString) throws IOException {
		m_writer.print(xmlString);
	}

	/**
	 * Escapes special characters. Characters below a certain range (defined by the space char) are removed (while these
	 * can be escaped, any valid XML parser will choke on them).
	 */
	public static String normalize(String s) {
		StringBuilder str = new StringBuilder();

		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);

			switch (ch) {
			case '<':
				str.append("&lt;");
				break;
			case '>':
				str.append("&gt;");
				break;
			case '&':
				str.append("&amp;");
				break;
			case '"':
				str.append("&quot;");
				break;
			case '\'':
				str.append("&apos;");
				break;
			case '\n':
				str.append("\n");
				break;
			case '\t':
				str.append("\t");
				break;
			case '\r':
				str.append("\r");
				break;
			default:
				if (ch >= ' ') {
					str.append(ch);
				}
			}
		}
		return str.toString();
	}

	/**
	 * Write a ResultSet response
	 */
	private void writeOneResultSet(TupleSet ts) throws IOException {
		m_writer.print("<r");
		String warning = ts.getWarning();
		if (warning != null) {
			m_writer.print(" m=\"" + warning + "\"");
		}
		List<String[]> data = ts.getData();
		int nrow = data.size();
		int ncol = 0;
		if (nrow > 0) {
			ncol = data.get(0).length;
		}
		if (ncol != 1) {
			m_writer.print(" c=\"" + ncol + "\"");
		}
		if (nrow != 1) {
			m_writer.print(" r=\"" + nrow + "\"");
		}
		if (ts.isEndOfResults()) {
			m_writer.print("><e/>\n");
		} else {
			m_writer.print(">\n");
		}

		for (String[] thisRow : data) {
			for (int i = 0; i < ncol; i++) {
				String colValue = thisRow[i];
				if (colValue != null) {
					m_writer.print("<v>" + normalize(colValue) + "</v>");
				} else {
					m_writer.print("<n/>");
				}
			}
			m_writer.print("\n");
		}

		m_writer.print("</r>\n");
	}
}
