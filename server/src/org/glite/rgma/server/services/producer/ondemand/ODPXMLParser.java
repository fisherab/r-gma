/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.ondemand;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.sql.DataType;
import org.glite.rgma.server.services.sql.DataType.Type;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.TupleSet;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Version of XMLConverter that uses SAX parser for increased performance.
 */
public class ODPXMLParser extends DefaultHandler {

	/** XML token that represents the &lt;col&gt; element. */
	public static final String RS_ROW_COL = "col";

	/** XML token that represents the &lt;row&gt; element. */
	public static final String RS_ROW = "row";

	/** XML token that represents the &lt;message&gt; element. */
	public static final String RS_RGMAWARNING_MESSAGE = "message";

	/** XML token that represents the &lt;XMLException&gt; element. */
	public static final String ODPEXCEPTION = "XMLException";

	/** XML token that represents the &lt;XMLResultSet&gt; element. */
	public static final String ODPRESULTSET = "XMLResultSet";

	/** XML token that represents the 'endOfResults' attribute. */
	public static final String RS_ENDOFRESULTS = "endOfResults";

	/** XML token that represents the 'metadata/column' element. */
	public static final String RS_COLUMNMETADATA = "metadata";

	/** XML token that represents the 'metadata/column/name' element. */
	public static final String RS_COLUMNMETADATA_NAME = "name";

	/** XML token that represents the 'metadata/column/type' element. */
	public static final String RS_COLUMNMETADATA_TYPE = "type";

	/** XML token that represents the 'metadata/column/table' element. */
	public static final String RS_COLUMNMETADATA_TABLE = "table";

	/** Object type: Exception. */
	public static final int EXCEPTION = 1;

	/** Object type: ResultSet. */
	public static final int RESULTSET = 2;

	/** Exception type: RGMAException. */
	public static final String TYPE_RGMA_EXCEPTION = "RGMAException";

	/** Exception type: UnknownResourceException. */
	public static final String TYPE_UNKNOWN_RESOURCE_EXCEPTION = "UnknownResourceException";

	/** Initialise a Logger for log4j logging. */
	private static final Logger LOG = Logger.getLogger(OnDemandProducerConstants.ONDEMAND_PRODUCER_LOGGER);
	
	ODPXMLParser(String xml) throws RGMAPermanentException {
		try {
			SAXParserFactory saxFactory = SAXParserFactory.newInstance();
			XMLReader xmlReader = saxFactory.newSAXParser().getXMLReader();
			xmlReader.setContentHandler(this);
			xmlReader.setErrorHandler(this);
			xmlReader.parse(new InputSource(new StringReader(xml)));
			if (getType() == ODPXMLParser.EXCEPTION) {
				throw getException();
			}
		} catch (SAXException e) {
			LOG.error("SAX error trying to parse XML: " + xml, e);
			throw new RGMAPermanentException("SAX error trying to parse XML: " + e.getMessage());
		} catch (IOException e) {
			LOG.error("I/O error trying to parse XML: " + xml, e);
			throw new RGMAPermanentException("I/O error trying to parse XML: " + e.getMessage());
		} catch (ParserConfigurationException e) {
			LOG.error("Parser configuration error trying to parse XML: " + xml, e);
			throw new RGMAPermanentException("Parser configuration error trying to parse XML. " + e.getMessage());
		}
	}
	
	/** The exception parsed from the XML. */
	private RGMAPermanentException m_exception;

	/** The ResultSet parsed from the XML. */
	private TupleSet m_resultSet;

	/** The message for the current exception. */
	private String m_exceptionMessage;

	/** The message for the current warning. */
	private String m_warningMessage;

	/** The contents of the current column. */
	private StringBuilder m_currentCol;

	/** The contents of the current column metadata. */
	private StringBuilder m_currentColMetaData;

	/** The current row data. */
	private List<String> m_rowData;

	/** Parser is inside colData. */
	private boolean m_inColData = false;

	/** Parser is inside colMetaData. */
	private boolean m_inColMetaData = false;

	/** Parser is inside message. */
	private boolean m_inMessage = false;

	/** Parser is inside warning message. */
	private boolean m_inWarningMessage = false;

	/** The type of object in the XML (RESULTSET or EXCEPTION). */
	private int m_objectType;

	/** The endOfResults flag for the current ResultSet. */
	private boolean m_endOfResults;

	/** The isNull flag for the current column. */
	private boolean m_isColumnNull;

	/** Parser is inside metadata column. */
	private boolean m_inMetadataColumn = false;

	/** Content of column metadata. */
	private StringBuilder m_columnMetadata;

	/** List of metadata column names. */
	private List<String> m_metadataColumnNames;

	/** List of metadata column types. */
	private List<DataType> m_metadataColumnTypes;

	/** List of metadata table names. */
	private List<String> m_metadataColumnTables;

	/** Parser is inside metadata column. */
	private boolean m_inMetadataColumnChars = false;

	private int m_columnCount;

	private OnDemandMetaData m_metaData;

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
	@Override
	public void characters(char[] ch, int start, int length) {
		if (m_objectType == RESULTSET) {
			if (m_inColMetaData) {
				m_currentColMetaData.append(new String(ch, start, length));
			} else if (m_inColData) {
				m_currentCol.append(new String(ch, start, length));
			} else if (m_inWarningMessage) {
				if (m_warningMessage == null) {
					m_warningMessage = new String(ch, start, length);
				} else {
					m_warningMessage += new String(ch, start, length);
				}
			} else if (m_inMetadataColumnChars) {
				m_columnMetadata.append(new String(ch, start, length));
			}
		} else if (m_inMessage) {
			if (m_exceptionMessage == null) {
				m_exceptionMessage = new String(ch, start, length);
			} else {
				m_exceptionMessage += new String(ch, start, length);
			}
		}
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
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if (m_objectType == RESULTSET) {
			if (qName.equals(RS_ROW)) {
				if (m_rowData.size() != m_columnCount) {
					throw new SAXException("Number of columns must match the metadata");
				}
				m_resultSet.addRow(m_rowData.toArray(new String[0]));
			} else if (qName.equals(RS_ROW_COL)) {
				if (m_isColumnNull) {
					m_rowData.add(null); // ArrayLists allow 'null' elements.
				} else {
					m_rowData.add(m_currentCol.toString());
				}
				m_inColData = false;
			} else if (qName.equals(RS_RGMAWARNING_MESSAGE)) {
				m_inWarningMessage = false;
			} else if (qName.equals(ODPRESULTSET)) {
				if (m_resultSet == null) {
					throw new SAXException("No metadata block found");
				}
				if (m_warningMessage != null) {
					m_resultSet.setWarning(m_warningMessage);
				}
				m_resultSet.setEndOfResults(m_endOfResults);
				m_objectType = 0;
			} else if (m_inMetadataColumn && qName.equals(RS_COLUMNMETADATA)) {
				m_inMetadataColumn = false;
				if (m_resultSet == null) {
					String[] metadataColumnNamesArray = m_metadataColumnNames.toArray(new String[0]);
					String[] metadataColumnTablesArray = m_metadataColumnTables.toArray(new String[0]);
					DataType[] metadataColumnTypesArray = m_metadataColumnTypes.toArray(new DataType[0]);
					try {
						m_metaData = new OnDemandMetaData(metadataColumnNamesArray, metadataColumnTablesArray, metadataColumnTypesArray);
						m_resultSet = new TupleSet();
					} catch (RGMAPermanentException e) {
						throw new SAXException(e);
					}
					m_columnCount = metadataColumnNamesArray.length;
				} else {
					throw new SAXException("Can't have two metadata blocks");
				}

			} else if (m_inMetadataColumn) {
				if (qName.equals(RS_COLUMNMETADATA_NAME)) {
					m_inMetadataColumnChars = false;
					m_metadataColumnNames.add(m_columnMetadata.toString());
				} else if (qName.equals(RS_COLUMNMETADATA_TYPE)) {
					m_inMetadataColumnChars = false;
					int typeInt = 0;
					try {
						typeInt = new Integer(m_columnMetadata.toString());
					} catch (NumberFormatException e) {
						throw new SAXException("Type in metadata could not be parsed as an integer");
					}
					DataType dt = null;
					Type type = null;
					try {
						type = Type.getType(typeInt);
					} catch (RGMAPermanentException e) {
						/* TODO something sensible */
					}
					if (type == Type.VARCHAR || type == Type.CHAR) {
						dt = new DataType(type, 255); // default size for varchars
						// and chars
					} else {
						dt = new DataType(type, 0);
					}
					m_metadataColumnTypes.add(dt);
				} else if (qName.equals(RS_COLUMNMETADATA_TABLE)) {
					m_inMetadataColumnChars = false;
					m_metadataColumnTables.add(m_columnMetadata.toString());
				}
			}
		} else if (m_objectType == EXCEPTION) {
			if (qName.equals(RS_RGMAWARNING_MESSAGE)) {
				m_inMessage = false;
			} else if (qName.equals(ODPEXCEPTION)) {
				m_exception = new RGMAPermanentException(m_exceptionMessage);
			}
		}
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
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		if (m_objectType == RESULTSET) {
			if (qName.equals(RS_ROW)) {
				if (m_metadataColumnNames == null) {
					throw new SAXException("No metadata specified");
				}
				m_rowData = new ArrayList<String>();
			} else if (qName.equals(RS_ROW_COL)) {
				String isNullValue = attributes.getValue("isNull");
				m_isColumnNull = (isNullValue != null) && isNullValue.equalsIgnoreCase("true");
				m_currentCol = new StringBuilder();
				m_inColData = true;
			} else if (qName.equals(RS_RGMAWARNING_MESSAGE)) {
				m_inWarningMessage = true;
			} else if (qName.equals(RS_COLUMNMETADATA)) {
				if (m_metadataColumnNames == null) {
					m_metadataColumnNames = new ArrayList<String>();
					m_metadataColumnTypes = new ArrayList<DataType>();
					m_metadataColumnTables = new ArrayList<String>();
				}
				m_inMetadataColumn = true;
			} else if (m_inMetadataColumn) {
				if (qName.equals(RS_COLUMNMETADATA_NAME) || qName.equals(RS_COLUMNMETADATA_TYPE) || qName.equals(RS_COLUMNMETADATA_TABLE)) {
					m_inMetadataColumnChars = true;
					m_columnMetadata = new StringBuilder();
				}
			}
		} else if (m_objectType == EXCEPTION) {
			if (qName.equals(RS_RGMAWARNING_MESSAGE)) {
				m_inMessage = true;
			}
		}

		if (qName.equals(ODPEXCEPTION)) {
			m_objectType = EXCEPTION;
		} else if (qName.equals(ODPRESULTSET)) {
			m_objectType = RESULTSET;

			String endOfResults = attributes.getValue(RS_ENDOFRESULTS);

			if (endOfResults != null) {
				m_endOfResults = new Boolean(endOfResults);
			} else {
				m_endOfResults = false;
			}
		}
	}

	private RGMAPermanentException getException() {
		return m_exception;
	}

	private int getType() {
		return m_objectType;
	}

	public TupleSet getTupleSet() {
		return m_resultSet;
	}

	public OnDemandMetaData getMetaData() {
		return m_metaData;
	}
}
