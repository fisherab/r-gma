/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

class XMLSAXConverter extends DefaultHandler {

	private RGMAException m_exception;

	private List<Tuple> m_data;

	private String m_warning;

	private StringBuilder m_currentCol;

	private String[] m_rowData;

	private int m_numCols;

	private boolean m_endOfResults;

	private int m_curCol;

	private boolean m_unknown;

	@Override
	public void characters(char[] ch, int start, int length) {
		if (m_currentCol != null) {
			m_currentCol.append(new String(ch, start, length));
		}
	}

	/**
	 * Generates a TupleSet derived from the XML string.
	 */
	public static TupleSet convertXMLResponse(String xml) throws UnknownResourceException, RGMAPermanentException, RGMATemporaryException {
		XMLSAXConverter converter = new XMLSAXConverter();
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			XMLReader xmlReader = saxFactory.newSAXParser().getXMLReader();
			xmlReader.setContentHandler(converter);
			xmlReader.setErrorHandler(converter);
			xmlReader.parse(new InputSource(new StringReader(xml)));
		} catch (SAXException e) {
			throw new RGMAPermanentException("SAX error trying to parse XML: " + e.getMessage());
		} catch (IOException e) {
			throw new RGMAPermanentException("I/O error trying to parse XML: " + e.getMessage());
		} catch (ParserConfigurationException e) {
			throw new RGMAPermanentException("Parser configuration error trying to parse XML: " + e.getMessage());
		}
		if (converter.m_unknown) {
			throw new UnknownResourceException();
		}
		if (converter.m_exception != null) {
			if (converter.m_exception instanceof RGMATemporaryException) {
				throw (RGMATemporaryException) converter.m_exception;
			} else {
				throw (RGMAPermanentException) converter.m_exception;
			}
		}
		return new TupleSet(converter.m_data, converter.m_endOfResults, converter.m_warning);
	}

	/**
	 * Generates a TupleSet derived from the XML string - will not return UnknownResourceExceptions
	 */
	public static TupleSet convertXMLResponseWithoutUnknownResource(String xml) throws RGMAPermanentException, RGMATemporaryException {
		try {
			return XMLSAXConverter.convertXMLResponse(xml);
		} catch (UnknownResourceException e) {
			throw new RGMAPermanentException("Internal error: " + e.getMessage());
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
			String numRows = attributes.getValue("r");
			if (numRows != null) {
				m_data = new ArrayList<Tuple>(Integer.parseInt(numRows));
			} else {
				m_data = new ArrayList<Tuple>(1);
			}
			m_curCol = m_numCols;
			m_warning = attributes.getValue("m");
			if (m_warning == null) {
				m_warning = "";
			}
		} else if (q == 'v') {
			m_currentCol = new StringBuilder();
		} else if (q == 'e') {
			m_endOfResults = true;
		} else if (q == 't' || q == 'p') {
			String numSuccessfulOps = attributes.getValue("o");
			if (q == 't') {
				m_exception = new RGMATemporaryException(attributes.getValue("m"));
			} else {
				m_exception = new RGMAPermanentException(attributes.getValue("m"));
			}
			if (numSuccessfulOps != null) {
				m_exception.setNumSuccessfulOps(Integer.parseInt(numSuccessfulOps));
			}
		} else if (q == 'u') {
			m_unknown = true;
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
				m_data.add(new Tuple(m_rowData));
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
