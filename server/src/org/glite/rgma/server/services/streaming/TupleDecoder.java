/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.streaming;

import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.TupleSetEnvelope;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Tuple decoder for the StandardStreamingProtocol. This class is not thread safe. Only one thread may access either of
 * the <code>pushBytes</code> and <code>popResults</code> methods at a time.
 */
public class TupleDecoder {

	/** Charset for encoding the streaming message */
	private static final String CHARSET_NAME = "UTF-8";

	/**
	 * A SAX parser which parses the XML result set.
	 */
	private class XmlResultSetParser extends DefaultHandler {

		/** The ResultSet parsed from the XML. */
		private TupleSet m_resultSet;

		private ResourceEndpoint m_source;

		private ResourceEndpoint m_target;

		private String[] m_rowData;

		private String m_query;

		private StringBuilder m_currentCol = new StringBuilder();

		private int m_numCols;

		private int m_curCol;

		private List<String[]> m_data;

		@Override
		public void characters(char[] ch, int start, int length) {
			m_currentCol.append(new String(ch, start, length));
		}

		public void endElement(String uri, String localName, String qName) {
			char q = qName.charAt(0);
			if (q == 'v' || q == 'n') {
				if (m_curCol == m_numCols) {
					m_rowData = new String[m_numCols];
					m_data.add(m_rowData);
					m_curCol = 0;
				}
				if (q == 'v') {
					m_rowData[m_curCol++] = m_currentCol.toString();
				} else {
					m_rowData[m_curCol++] = null;
				}
			}
		}

		/**
		 * Returns the first (or only) ResultSet parsed.
		 * 
		 * @return A ResultSet created from the XML
		 */
		public TupleSetEnvelope getResultSet() {
			return new TupleSetEnvelope(m_resultSet, m_source, m_target, m_query);
		}

		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			try {
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
						m_resultSet = new TupleSet(Integer.parseInt(numRows));
					} else {
						m_resultSet = new TupleSet(1);
					}
					m_data = m_resultSet.getData();
					m_curCol = m_numCols;
					m_resultSet.setWarning(attributes.getValue("m"));
					m_query = attributes.getValue("q");
					m_source = new ResourceEndpoint(new URL(attributes.getValue("su")), Integer.parseInt(attributes.getValue("si")));
					m_target = new ResourceEndpoint(new URL(attributes.getValue("tu")), Integer.parseInt(attributes.getValue("ti")));
				} else if (q == 'v') {
					m_currentCol.setLength(0);
				} else if (q == 'e' && m_resultSet != null) {
					m_resultSet.setEndOfResults(true);
				} else if (q == 'n') {
					// Nothing to do
				} else {
					throw new SAXException("Unexpected tag " + qName + " in streamed XML from server.");
				}
			} catch (NumberFormatException e) {
				throw new SAXException(e.getMessage());
			} catch (RGMAPermanentException e) {
				throw new SAXException(e.getMessage());
			} catch (MalformedURLException e) {
				throw new SAXException(e.getMessage());
			}
		}
	}

	/**
	 * A SAX parser which parses the XML result set.
	 */
	private class OldXmlResultSetParser extends DefaultHandler {

		private static final String ELE_ROOT = "rs";

		private static final String ATTR_END = "e";

		private static final String ELE_METADATA = "meta";

		private static final String ELE_COL = "c";

		private static final String ELE_NAME = "n";

		private static final String ELE_TYPE = "t";

		private static final String ELE_SIZE = "s";

		private static final String ELE_TABLE = "tab";

		private static final String ELE_SOURCE = "src";

		private static final String ELE_TARGET = "tgt";

		private static final String ELE_QUERY = "q";

		private static final String ELE_URL = "u";

		private static final String ELE_ID = "id";

		private static final String ELE_ROW = "r";

		private static final String ATTR_NULL = "null";

		private static final String ELE_WARNING = "w";

		private static final String VAL_TRUE = "t";

		private Mode m_mode;

		private StringBuffer m_chars;

		/** The ResultSet parsed from the XML. */
		private TupleSet m_resultSet;

		private ResourceEndpoint m_source;

		private ResourceEndpoint m_target;

		private List<List<String>> m_rows;

		private List<String> m_rowData;

		private boolean m_null;

		private String m_url;

		private int m_id;

		private String m_warning;

		private boolean m_end;

		private String m_query;

		/**
		 * Constructor.
		 */
		public OldXmlResultSetParser() {
			m_mode = Mode.START;
			m_chars = new StringBuffer();
		}

		/**
		 * Process characters
		 * 
		 * @param ch
		 *            Array of characters
		 * @param start
		 *            Start index
		 * @param length
		 *            Number of chars.
		 */
		public void characters(char[] ch, int start, int length) {
			m_chars.append(ch, start, length);
		}

		/**
		 * Process end tag.
		 * 
		 * @param uri
		 *            URI of tag
		 * @param localName
		 *            Local name
		 * @param qName
		 *            Fully qualified name
		 */
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (qName.equals(ELE_ROOT)) {
				m_resultSet = new TupleSet();
				for (List<String> row : m_rows) {
					m_resultSet.addRow((String[]) row.toArray(new String[0]));
				}
				m_resultSet.setEndOfResults(m_end);
				if (m_warning != null) {
					m_resultSet.setWarning(m_warning);
				}
				m_mode = Mode.START;
			} else if (qName.equals(ELE_METADATA)) {
				m_mode = Mode.ROOT;
			} else if (qName.equals(ELE_COL)) {
				if (m_mode == Mode.COL_METADATA) {
					m_mode = Mode.METADATA;
				} else {
					if (m_null) {
						m_rowData.add(null);
					} else {
						m_rowData.add(m_chars.toString());
					}
					m_mode = Mode.ROW;
				}
			} else if (qName.equals(ELE_NAME)) {
				m_mode = Mode.COL_METADATA;
			} else if (qName.equals(ELE_TYPE)) {
				m_mode = Mode.COL_METADATA;
			} else if (qName.equals(ELE_SIZE)) {
				m_mode = Mode.COL_METADATA;
			} else if (qName.equals(ELE_TABLE)) {
				m_mode = Mode.COL_METADATA;
			} else if (qName.equals(ELE_SOURCE)) {
				try {
					m_source = new ResourceEndpoint(new URL(m_url), m_id);
				} catch (MalformedURLException e) {
					throw new SAXException("Source URL: " + m_url + " is not a valid URL");
				} catch (RGMAPermanentException e) {
					throw new SAXException("Unexpected RGMAPermanentException ", e);
				}
				m_mode = Mode.METADATA;
			} else if (qName.equals(ELE_TARGET)) {
				try {
					m_target = new ResourceEndpoint(new URL(m_url), m_id);
				} catch (MalformedURLException e) {
					throw new SAXException("Target URL: " + m_url + " is not a valid URL");
				} catch (RGMAPermanentException e) {
					throw new SAXException("Unexpected RGMAPermanentException ", e);
				}
				m_mode = Mode.METADATA;
			} else if (qName.equals(ELE_URL)) {
				m_url = m_chars.toString();
				if (m_mode == Mode.SOURCE_URL_METADATA) {
					m_mode = Mode.SOURCE_METADATA;
				} else {
					m_mode = Mode.TARGET_METADATA;
				}
			} else if (qName.equals(ELE_ID)) {
				try {
					m_id = Integer.parseInt(m_chars.toString());
				} catch (NumberFormatException e) {
					throw new SAXException("Resource ID " + m_chars + " is not an integer");
				}
				if (m_mode == Mode.SOURCE_ID_METADATA) {
					m_mode = Mode.SOURCE_METADATA;
				} else {
					m_mode = Mode.TARGET_METADATA;
				}
			} else if (qName.equals(ELE_QUERY)) {
				m_query = m_chars.toString();
				m_mode = Mode.METADATA;
			} else if (qName.equals(ELE_ROW)) {
				m_rows.add(m_rowData);
				m_mode = Mode.ROOT;
			} else if (qName.equals(ELE_WARNING)) {
				m_warning = m_chars.toString();
				m_mode = Mode.ROOT;
			}
		}

		/**
		 * Returns the first (or only) ResultSet parsed.
		 * 
		 * @return A ResultSet created from the XML
		 */
		public TupleSetEnvelope getResultSet() {
			return new TupleSetEnvelope(m_resultSet, m_source, m_target, m_query);
		}

		/**
		 * Process start tag
		 * 
		 * @param uri
		 *            URI of tag
		 * @param localName
		 *            Local name
		 * @param qName
		 *            Fully qualified name
		 * @param attributes
		 *            Attributes of tag
		 */
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			if (qName.equals(ELE_ROOT)) {
				if (m_mode != Mode.START) {
					unexpectedElement(qName);
				}
				String endAttr = attributes.getValue(ATTR_END);
				m_end = (endAttr != null) && endAttr.equalsIgnoreCase(VAL_TRUE);
				m_rows = new ArrayList<List<String>>();
				m_mode = Mode.ROOT;
			} else if (qName.equals(ELE_METADATA)) {
				m_mode = Mode.METADATA;
			} else if (qName.equals(ELE_COL)) {
				if (m_mode == Mode.METADATA) {
					m_mode = Mode.COL_METADATA;
				} else if (m_mode == Mode.ROW) {
					String nullAttr = attributes.getValue(ATTR_NULL);
					m_null = (nullAttr != null) && nullAttr.equalsIgnoreCase(VAL_TRUE);
					m_mode = Mode.COL;
				} else {
					unexpectedElement(qName);
				}
			} else if (qName.equals(ELE_NAME)) {
				if (m_mode != Mode.COL_METADATA) {
					unexpectedElement(qName);
				}
				m_mode = Mode.COL_NAME_METADATA;
			} else if (qName.equals(ELE_TYPE)) {
				if (m_mode != Mode.COL_METADATA) {
					unexpectedElement(qName);
				}
				m_mode = Mode.COL_TYPE_METADATA;
			} else if (qName.equals(ELE_SIZE)) {
				if (m_mode != Mode.COL_METADATA) {
					unexpectedElement(qName);
				}
				m_mode = Mode.COL_TYPE_METADATA;
			} else if (qName.equals(ELE_TABLE)) {
				if (m_mode != Mode.COL_METADATA) {
					unexpectedElement(qName);
				}
				m_mode = Mode.COL_TABLE_METADATA;
			} else if (qName.equals(ELE_SOURCE)) {
				if (m_mode != Mode.METADATA) {
					unexpectedElement(qName);
				}
				m_mode = Mode.SOURCE_METADATA;
			} else if (qName.equals(ELE_TARGET)) {
				if (m_mode != Mode.METADATA) {
					unexpectedElement(qName);
				}
				m_mode = Mode.TARGET_METADATA;
			} else if (qName.equals(ELE_URL)) {
				if (m_mode == Mode.SOURCE_METADATA) {
					m_mode = Mode.SOURCE_URL_METADATA;
				} else if (m_mode == Mode.TARGET_METADATA) {
					m_mode = Mode.TARGET_URL_METADATA;
				} else {
					unexpectedElement(qName);
				}
			} else if (qName.equals(ELE_ID)) {
				if (m_mode == Mode.SOURCE_METADATA) {
					m_mode = Mode.SOURCE_ID_METADATA;
				} else if (m_mode == Mode.TARGET_METADATA) {
					m_mode = Mode.TARGET_ID_METADATA;
				} else {
					unexpectedElement(qName);
				}
			} else if (qName.equals(ELE_QUERY)) {
				if (m_mode != Mode.METADATA) {
					unexpectedElement(qName);
				}
				m_mode = Mode.QUERY_METADATA;
			} else if (qName.equals(ELE_ROW)) {
				if (m_mode != Mode.ROOT) {
					unexpectedElement(qName);
				}
				m_rowData = new ArrayList<String>();
				m_mode = Mode.ROW;
			} else if (qName.equals(ELE_WARNING)) {
				if (m_mode != Mode.ROOT) {
					unexpectedElement(qName);
				}
				m_mode = Mode.WARNING;
			}

			m_chars.setLength(0);
		}

		private void unexpectedElement(String element) throws SAXException {
			throw new SAXException("Got unexpected element '" + element + "' in mode " + m_mode);
		}
	}

	enum Mode {
		START, ROOT, METADATA, COL_METADATA, COL_NAME_METADATA, COL_TYPE_METADATA, COL_TABLE_METADATA, SOURCE_METADATA, SOURCE_URL_METADATA, SOURCE_ID_METADATA, TARGET_METADATA, TARGET_URL_METADATA, TARGET_ID_METADATA, QUERY_METADATA, ROW, COL, WARNING
	}

	/** Size of character buffer. */
	private static final int CHAR_BUFFER_SIZE = 4096;

	/** List of result sets that have been decoded but not yet popped. */
	private List<TupleSetEnvelope> m_results;

	/** A string buffer containing XML for the current result set */
	private StringBuffer m_currentXml;

	/** Buffer containing characters decoded from the byte stream */
	private CharBuffer m_charBuffer;

	/** Converts bytes into characters. */
	private CharsetDecoder m_decoder;

	private int m_headerInt;

	/**
	 * Decodes a byte stream from a streaming connection into ResultSets. Since the bytes may arrive in chunks of any
	 * size which do not necessarily correspond to complete ResultSet object, the pushing of bytes and the popping of
	 * decoded result sets must be separate. Pushing bytes may not result in any new complete ResultSets being decoded.
	 * 
	 * @param headerInt
	 */
	public TupleDecoder(int headerInt) throws RGMAPermanentException {
		m_results = new ArrayList<TupleSetEnvelope>();
		m_currentXml = new StringBuffer();
		m_charBuffer = ByteBuffer.allocateDirect(CHAR_BUFFER_SIZE).asCharBuffer();
		m_headerInt = headerInt;
		Charset cs = Charset.availableCharsets().get(CHARSET_NAME);
		if (cs == null) {
			throw new RGMAPermanentException(CHARSET_NAME + " charset is not available");
		} else {
			m_decoder = cs.newDecoder();
		}
	}

	/**
	 * Retrieve decoded result sets. This method may return zero or more complete ResultSet objects. Normally it should
	 * be called directly after a call to pushBytes. However if the bytes pushed do not contain a complete ResultSet, no
	 * results may be returned until further bytes from the stream are pushed.
	 * 
	 * @return List of ResultSet objects which may be empty.
	 * @throws RGMAPermanentException
	 *             If the decoder encountered an unrecoverable error.
	 */
	public List<TupleSetEnvelope> popResults() {
		List<TupleSetEnvelope> ret = m_results;
		m_results = new ArrayList<TupleSetEnvelope>();
		return ret;
	}

	/**
	 * Decode bytes into result sets. The bytes passed must be processed by the decoder and be available for deletion
	 * afterwards. If the decoder cannot turn any of the bytes into a complete result set (for example because they only
	 * represent part of result set), it must take a copy of those bytes which could not be processed.
	 * 
	 * @param bytes
	 *            ByteBuffer positioned at the point where the decoder should start reading.
	 * @throws RGMAPermanentException
	 *             If the decoder encountered an unrecoverable error. Decoders should not throw an exception on
	 *             malformed input if it is possible to recover (e.g. by skipping a result set). A warning should be
	 *             logged instead when tuples may have been lost.
	 */
	public void pushBytes(ByteBuffer bytes) throws RGMAPermanentException {
		while (bytes.remaining() > 0) {
			CoderResult result = m_decoder.decode(bytes, m_charBuffer, false);
			if (result.isOverflow()) {
				// Not enough room in the output buffer
				m_charBuffer.flip();
				flush();
			}
		}

		// Parse any remaining results in the char buffer
		m_charBuffer.flip();
		flush();
	}

	/**
	 * Flush the data in the current character buffer into a string buffer, parsing any complete result sets.
	 */
	private void flush() throws RGMAPermanentException {
		String currentChars = m_charBuffer.toString();
		int separatorIndex;

		while ((separatorIndex = currentChars.indexOf(0)) >= 0) {
			// Have found an end-of-result-set marker.
			// First, append the final characters to the XML buffer, then parse it.
			m_currentXml.append(currentChars.substring(0, separatorIndex));
			TupleSetEnvelope rs = parse(m_currentXml.toString());
			m_results.add(rs);
			// Remove the consumed characters
			if (separatorIndex == currentChars.length() - 1) {
				currentChars = "";
			} else {
				currentChars = currentChars.substring(separatorIndex + 1);
			}
			m_currentXml.setLength(0);
		}

		// Append any remaining characters that were not part of a complete result set to the
		// XML buffer.
		m_currentXml.append(currentChars);

		m_charBuffer.clear();
	}

	/**
	 * Parse XML to return a result set object.
	 */
	private TupleSetEnvelope parse(String xml) throws RGMAPermanentException {
		if (xml.trim().equals("")) {
			throw new RGMAPermanentException("No content found in streamed XML result set");
		}
		TupleSetEnvelope resultSet = null;
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		try {
			XMLReader xmlReader = saxFactory.newSAXParser().getXMLReader();
			if (m_headerInt == 1) {
				OldXmlResultSetParser parser = new OldXmlResultSetParser();
				xmlReader.setContentHandler(parser);
				xmlReader.setErrorHandler(parser);
				xmlReader.parse(new InputSource(new StringReader(xml)));
				resultSet = parser.getResultSet();
			} else {
				XmlResultSetParser parser = new XmlResultSetParser();
				xmlReader.setContentHandler(parser);
				xmlReader.setErrorHandler(parser);
				xmlReader.parse(new InputSource(new StringReader(xml)));
				resultSet = parser.getResultSet();
			}

			if (resultSet != null) {
				return resultSet;
			} else {
				throw new RGMAPermanentException("No result sets found in streamed XML result set");
			}
		} catch (SAXException e) {
			throw new RGMAPermanentException("Error parsing streamed XML result set", e);
		} catch (IOException e) {
			throw new RGMAPermanentException("IO error parsing streamed XML result set", e);
		} catch (ParserConfigurationException e) {
			throw new RGMAPermanentException("Parser error parsing streamed XML result set", e);
		}
	}
}
