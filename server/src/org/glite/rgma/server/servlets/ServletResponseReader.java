/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.servlets;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.glite.rgma.server.system.NumericException;
import org.glite.rgma.server.system.RGMAException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.UnknownResourceException;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Reads the response from an R-GMA servlet request.
 */
public class ServletResponseReader {

	private RSP m_parser;

	public List<TupleSet> readResultSets(String response) throws UnknownResourceException, RGMATemporaryException, RGMAPermanentException, NumericException {
		try {
			process(response);
		} catch (NumericException e) {
			throw new RGMAPermanentException(e.getMessage() + " number: " + e.getNumber());
		}
		return m_parser.m_resultSets;
	}

	public TupleSet readResultSet(String response) throws UnknownResourceException, RGMATemporaryException, RGMAPermanentException, NumericException {
		process(response);
		TupleSet resultSet = m_parser.m_resultSet;
		if (resultSet != null) {
			return resultSet;
		} else {
			throw new RGMAPermanentException("No result set found");
		}
	}

	private void process(String response) throws RGMATemporaryException, RGMAPermanentException, UnknownResourceException, NumericException {
		try {
			if (response.startsWith("<?xml")) {
				m_parser = new OldXmlTupleSetParser();
			} else {
				m_parser = new XmlTupleSetParser();
			}
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			XMLReader xmlReader = saxFactory.newSAXParser().getXMLReader();
			xmlReader.setContentHandler(m_parser);
			xmlReader.setErrorHandler(m_parser);
			xmlReader.parse(new InputSource(new StringReader(response)));

			if (m_parser.m_exception != null) {
				Exception e = m_parser.m_exception;
				if (e instanceof RGMAPermanentException) {
					throw (RGMAPermanentException) e;
				} else if (e instanceof RGMATemporaryException) {
					throw (RGMATemporaryException) e;
				} else if (e instanceof UnknownResourceException) {
					throw (UnknownResourceException) e;
				} else if (e instanceof NumericException) {
					throw (NumericException) e;
				} else {
					throw new RGMAPermanentException("Unexpected error parsing XML response: " + e.getMessage(), e);
				}
			}
		} catch (NumberFormatException e) {
			throw new RGMAPermanentException("Error parsing XML response", e);
		} catch (SAXException e) {
			throw new RGMAPermanentException("Error parsing XML response", e);
		} catch (IOException e) {
			throw new RGMAPermanentException("IO error parsing XML response", e);
		} catch (ParserConfigurationException e) {
			throw new RGMAPermanentException("Parser error parsing XML response", e);
		}
	}

	private enum ResponseType {
		EXCEPTION, RESULTSETS
	}

	private class RSP extends DefaultHandler {
		/** The exception parsed from the XML. */
		protected Exception m_exception;

		/** The last TupleSet parsed from the XML. */
		protected TupleSet m_resultSet;

		/** A list of the TupleSets from the XML */
		protected final List<TupleSet> m_resultSets = new ArrayList<TupleSet>();
	}

	/**
	 * To simplify coding this may throw null pointer and number format exceptions.
	 */
	private class XmlTupleSetParser extends RSP {

		/** The current row data. */
		private String[] m_rowData;

		/** Number of columns */
		private int m_numCols;

		private StringBuilder m_currentCol;

		private int m_curCol;

		@Override
		public void characters(char[] ch, int start, int length) {
			if (m_currentCol != null) {
				m_currentCol.append(new String(ch, start, length));
			}
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) {
			if (m_exception != null) {
				return;
			}
			char q = qName.charAt(0);
			if (qName.length() != 1) {
				q = ' ';
			}
			if (q == 'r') {
				String numCols = attributes.getValue("c");
				if (numCols != null) {
					m_numCols = Integer.parseInt(numCols);
				} else {
					m_numCols = 1;
				}
				m_resultSet = new TupleSet();
				m_resultSets.add(m_resultSet);

				m_curCol = m_numCols;
				String warning = attributes.getValue("m");
				if (warning != null) {
					m_resultSet.setWarning(warning);
				}
			} else if (q == 'v') {
				m_currentCol = new StringBuilder();
			} else if (q == 'e') {
				m_resultSet.setEndOfResults(true);
			} else if (q == 't' || q == 'p') {
				String numSuccessfulOps = attributes.getValue("o");
				RGMAException e;
				if (q == 't') {
					e = new RGMATemporaryException(attributes.getValue("m"));
				} else {
					e = new RGMAPermanentException(attributes.getValue("m"));
				}
				if (numSuccessfulOps != null) {
					e.setNumSuccessfulOps(Integer.parseInt(numSuccessfulOps));
				}
				m_exception = e;
			} else if (q == 'u') {
				m_exception = new UnknownResourceException("Resource not found ...");
			} else if (q == 'i') {
				m_exception = new NumericException(Integer.parseInt(attributes.getValue("n")), attributes.getValue("m"));
			} else if (q == 's' || q == 'n') {
				// Nothing to do
			} else {
				m_exception = new RGMAPermanentException("Unexpected tag " + qName + " in XML from server.");
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) {
			if (m_exception != null) {
				return;
			}
			char q = qName.charAt(0);
			if (q == 'v' || q == 'n') {
				if (m_curCol == m_numCols) {
					m_rowData = new String[m_numCols];
					m_resultSet.addRow(m_rowData);
					m_curCol = 0;
				}
				if (q == 'v') {
					m_rowData[m_curCol++] = m_currentCol.toString();
				} else {
					m_rowData[m_curCol++] = null;
				}
			}
		}
	}

	private class OldXmlTupleSetParser extends RSP {

		/** The message for the current exception. */
		private String m_exceptionMessage;

		/** The type for the current exception. */
		private String m_type;

		/** The current row data. */
		private String[] m_rowData;

		/** The current column number. */
		private int m_colNum;

		/** The type of object in the XML (RESULTSET or EXCEPTION). */
		private ResponseType m_responseType;

		/** The endOfResults flag for the current TupleSet. */
		private boolean m_endOfResults;

		/** The isNull flag for the current column. */
		private boolean m_isColumnNull;

		/** The numSuccessfulOps flag for the current exception. */
		private String m_numSuccessfulOps;

		/** Current text being built */
		private String m_text;

		/** Number of columns */
		private int m_numCols;

		/** Error number in the exception */
		private int m_errNum;

		/**
		 * Process characters. Note that the parser is allowed to break the text into chunks. The String constructor
		 * used below requires no copying. If there should be a second chunk a copy will be required - however this does
		 * not seem to happen in practice.
		 * 
		 * @param ch
		 *            Array of characters
		 * @param start
		 *            Start index
		 * @param length
		 *            Number of chars.
		 */
		@Override
		public void characters(char[] ch, int start, int length) {
			if (m_text == null) {
				return;
			}
			if (m_text.length() == 0) {
				m_text = new String(ch, start, length);
			} else {
				m_text = m_text + new String(ch, start, length);
			}
		}

		/**
		 * Process start tag
		 */
		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) {
			if (m_responseType == ResponseType.RESULTSETS) {

				if (qName.equals("col")) {
					String isNullValue = attributes.getValue("isNull");
					m_isColumnNull = isNullValue != null && isNullValue.equalsIgnoreCase("true");
					m_text = new String();

				} else if (qName.equals("row")) {
					m_rowData = new String[m_numCols];
					m_colNum = 0;

				} else if (qName.equals("metadata") || qName.equals("rowMetaData")) {
					m_colNum = 0;

				} else if (qName.equals("name") || qName.equals("type") || qName.equals("table") || qName.equals("colMetaData") || qName.equals("message")) {
					m_text = new String();
				}

			} else if (m_responseType == ResponseType.EXCEPTION) {

				if (qName.equals("message")) {
					m_text = new String();
				}

			} else {

				if (qName.equals("XMLResultSet")) {
					m_responseType = ResponseType.RESULTSETS;
					String endOfResults = attributes.getValue("endOfResults");
					if (endOfResults != null) {
						m_endOfResults = new Boolean(endOfResults).booleanValue();
					} else {
						m_endOfResults = false;
					}

				} else if (qName.equals("XMLException")) {
					m_responseType = ResponseType.EXCEPTION;
					m_errNum = Integer.parseInt(attributes.getValue("errorNumber"));
					m_numSuccessfulOps = attributes.getValue("numSuccessfulOps");
					m_type = attributes.getValue("type");
				}
			}
		}

		/**
		 * Process end tag.
		 */
		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (m_responseType == ResponseType.RESULTSETS) {

				if (qName.equals("col")) {
					if (m_isColumnNull) {
						m_rowData[m_colNum] = null;
					} else {
						m_rowData[m_colNum] = m_text;
						m_text = null;
					}
					m_colNum++;

				} else if (qName.equals("row")) {
					m_resultSet.addRow(m_rowData);

				} else if (qName.equals("type")) {
					m_text = null;

				} else if (qName.equals("name") || qName.equals("colMetaData")) {
					m_text = null;
					if (qName.equals("colMetaData")) {
						m_colNum++;
					}

				} else if (qName.equals("table")) {
					m_text = null;

				} else if (qName.equals("column")) {
					m_colNum++;

				} else if (qName.equals("metadata") || qName.equals("rowMetaData")) {
					m_resultSet = new TupleSet();
					m_resultSet.setEndOfResults(m_endOfResults);
					m_resultSets.add(m_resultSet);
					m_numCols = m_colNum;

				} else if (qName.equals("message")) {
					m_resultSet.setWarning(m_text.toString());
					m_text = null;
				}

			} else if (m_responseType == ResponseType.EXCEPTION) {

				if (qName.equals("message")) {
					m_exceptionMessage = m_text;
					m_text = null;

				} else if (qName.equals("XMLException")) {
					if (m_type.equals("UnknownResourceException")) {
						m_exception = new UnknownResourceException(m_exceptionMessage);
					} else {
						RGMAException rgmaException = createRGMAException(m_errNum, m_exceptionMessage);
						if (m_numSuccessfulOps != null) {
							rgmaException.setNumSuccessfulOps(Integer.parseInt(m_numSuccessfulOps));
						}
						m_exception = rgmaException;
					}
				}
			}
		}

		/**
		 * Creates the appropriate RGMAException based on the error number.
		 */
		private RGMAException createRGMAException(int errNum, String message) {
			if (errNum == 3 || errNum == 7 || errNum == 9) {
				return new RGMATemporaryException(message);
			} else {
				return new RGMAPermanentException(message);
			}
		}
	}
}
