/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.browser.client;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.NamedNodeMap;
import com.google.gwt.xml.client.Node;
import com.google.gwt.xml.client.NodeList;
import com.google.gwt.xml.client.XMLParser;

/**
 * Version of XMLConverter that uses SAX parser for increased performance.
 */
class XMLConverter {
	
	/**
	 * Generates a TupleSet derived from the XML string.
	 */
	public static TupleSet convertXMLResponse(String xml) throws RGMAException, UnknownResourceException {
		final XMLConverter converter = XMLConverter.commonConvert(xml);
		return converter.m_tupleSet;
	}

	/**
	 * Generates a TupleSet derived from the XML string.
	 */
	public static TupleSet convertXMLResponseWithoutUnknownResource(String xml) throws RGMAException {
		XMLConverter converter;
		try {
			converter = XMLConverter.commonConvert(xml);
		} catch (UnknownResourceException e) {
			throw new RGMAPermanentException(e.getMessage());
		}
		return converter.m_tupleSet;
	}

	private static XMLConverter commonConvert(String xml) throws RGMAException, UnknownResourceException {
				
		final XMLConverter converter = new XMLConverter();

		final Element de = XMLParser.parse(xml).getDocumentElement();
				
		String nodeName = de.getNodeName();
		if (nodeName.length() != 1) {
			throw new RGMAPermanentException("Unexpected node name " + nodeName + " found in XML from server " + de);
		}
		char nN = nodeName.charAt(0);
		if (nN == 'r') {
			NamedNodeMap atts = de.getAttributes();

			Node att = atts.getNamedItem("m");
			String warning = "";
			if (att != null) {
				warning = att.getNodeValue();
			}

			att = atts.getNamedItem("r");
			int numRows = 1;
			if (att != null) {
				numRows = Integer.parseInt(att.getNodeValue());
			}

			att = atts.getNamedItem("c");
			int numCols = 1;
			if (att != null) {
				numCols = Integer.parseInt(att.getNodeValue());
			}

			List<String[]> data = new ArrayList<String[]>(numRows);
			String[] rowData = null;
			final NodeList vens = de.getChildNodes();
			int i = 0;
			int j = 0;
			boolean eor = false;
			for (int k = 0; k < vens.getLength(); k++) {
				Node ven = vens.item(k);
				if (ven.getNodeType() == Node.ELEMENT_NODE) {
;
					nodeName = ven.getNodeName();
					if (nodeName.length() != 1) {
						throw new RGMAPermanentException("Unexpected node name " + nodeName + " found in XML from server " + de);
					}
					nN = nodeName.charAt(0);

					if (nN == 'e') {
						eor = true;
					} else {
						if (j == 0) {
							if (i == numRows) {
								throw new RGMAPermanentException("More data returned in XML tuple set than specified in header");
							}
							rowData = new String[numCols];
						}

						if (nN == 'v') {
							Node text = ven.getFirstChild();
							if (text != null) {
								rowData[j] = text.getNodeValue();
							} else {
								rowData[j] = "";
							}
						} else { /* n */
							rowData[j] = null;
						}

						if (++j == numCols) {
							data.add(rowData);
							j = 0;
							i++;
						}
					}
				}
			}

			converter.m_tupleSet = new TupleSet(data, eor, warning);

		} else if (nN == 'u') {

			throw new UnknownResourceException();

		} else if (nN == 'p' || nN == 't') {

			NamedNodeMap atts = de.getAttributes();

			RGMAException e;
			String msg = atts.getNamedItem("m").getNodeValue();

			if (nN == 'p') {
				e = new RGMAPermanentException(msg);
			} else { /* 't */
				e = new RGMATemporaryException(msg);
			}
			Node att = atts.getNamedItem("o");
			if (att != null) {
				e.setNumSuccessfulOps(Integer.parseInt(att.getNodeValue()));
			}
			throw e;

		} else {
			throw new RGMAPermanentException("Unexpected node name " + nN + " found in XML from server " + de);
		}
		return converter;
	}

	private TupleSet m_tupleSet;

}
