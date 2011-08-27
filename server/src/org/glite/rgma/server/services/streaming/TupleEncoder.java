/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.streaming;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.glite.rgma.server.servlets.ServletResponseWriter;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TupleSet;
import org.glite.rgma.server.system.TupleSetEnvelope;

/**
 * Tuple encoder for the StandardStreamingProtocol.
 */
public class TupleEncoder {

	/** Byte token used as a result set separator */
	private static final byte NULL = 0;

	/** Charset for encoding the streaming message */
	private static final String CHARSET_NAME = "UTF-8";

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

	private static final String VAL_FALSE = "f";

	private int m_protVersion;

	public TupleEncoder(int protVersion) {
		m_protVersion = protVersion;
	}

	/**
	 * Encode a result set into bytes.
	 * 
	 * @param results
	 *            containing tuples to encode.
	 * @return ByteBuffer containing the encoded result set, with position set to the beginning, ready to read.
	 * @throws RGMAPermanentException
	 *             If the result set could not be encoded.
	 */
	public ByteBuffer encode(TupleSetEnvelope results) throws RGMAPermanentException {
		byte[] bytes;
		try {
			if (m_protVersion ==1) {
				bytes = oldToXML(results).getBytes(CHARSET_NAME);
			} else {
				bytes = toXML(results).getBytes(CHARSET_NAME);
			}
			
		} catch (UnsupportedEncodingException e) {
			throw new RGMAPermanentException(CHARSET_NAME + " encoding not supported by JVM");
		}

		ByteBuffer ret = ByteBuffer.allocate(bytes.length + 1);
		ret.put(bytes);
		ret.put(NULL);
		return ret;
	}

	/**
	 * Get the header bytes. This header should be sent at the start of every new connection. It should be a 4 byte
	 * integer, normally representing the streaming protocol version number.
	 * 
	 * @return ByteBuffer containing the 4 byte header, with position set to the beginning of the header, ready to read.
	 * @throws RGMAPermanentException
	 *             If the header could not be constructed.
	 */
	public ByteBuffer getHeader() {
		ByteBuffer header = ByteBuffer.allocate(4);
		header.putInt(m_protVersion);
		return header;
	}


	/**
	 * Convert a result set into XML format.
	 */
	private String toXML(TupleSetEnvelope results) throws RGMAPermanentException {
		TupleSet ts = results.getTupleSet();
		
		ResourceEndpoint ep = results.getSource();
		String surl = ServletResponseWriter.normalize(ep.getURL().toString());
		int sid = ep.getResourceID();
		ep = results.getTarget();
		String turl = ServletResponseWriter.normalize(ep.getURL().toString());
		int tid = ep.getResourceID();
		
		StringBuilder buffer = new StringBuilder("<r" + " q=\"" + ServletResponseWriter.normalize(results.getQuery()) + "\""   + " si=\"" + sid + "\"" + " su=\"" + surl + "\"" + " ti=\"" + tid + "\"" + " tu=\"" + turl + "\"");
		String warning = ts.getWarning();
		if (warning != null) {
			buffer.append(" m=\"" + warning + "\"");
		}
		List<String[]> data = ts.getData();
		int nrow = data.size();
		int ncol = 0;
		if (nrow > 0) {
			ncol = data.get(0).length;	
		}
		if (ncol != 1) {
			buffer.append(" c=\"" + ncol + "\"");
		}
		if (nrow != 1) {
			buffer.append(" r=\"" + nrow + "\"");
		}
		if (ts.isEndOfResults()) {
			buffer.append("><e/>\n");
		} else {
			buffer.append(">\n");
		}

		for (String[] thisRow : data) {
			for (int i = 0; i < ncol; i++) {
				String colValue = thisRow[i];
				if (colValue != null) {
					buffer.append("<v>" + ServletResponseWriter.normalize(colValue) + "</v>");
				} else {
					buffer.append("<n/>");
				}
			}
			buffer.append("\n");
		}
		
		return buffer.append("</r>\n").toString();
	}
	
	/**
	 * Convert a result set into XML format.
	 */
	private String oldToXML(TupleSetEnvelope results) throws RGMAPermanentException {
		TupleSet ts = results.getTupleSet();
		StringBuffer buffer = new StringBuffer(1024);
		buffer.append("<").append(ELE_ROOT).append(" ").append(ATTR_END).append("=\"");
		if (ts.isEndOfResults()) {
			buffer.append(VAL_TRUE).append("\">");
		} else {
			buffer.append(VAL_FALSE).append("\">");
		}
		List<String[]> data = ts.getData();

		int columnCount = data.size() > 0 ? data.get(0).length : 1;
		buffer.append("<").append(ELE_METADATA).append(">");
		for (int i = 1; i <= columnCount; i++) {
			buffer.append("<").append(ELE_COL).append(">");
			buffer.append("<").append(ELE_NAME).append(">").append("").append("</").append(ELE_NAME).append(">");
			buffer.append("<").append(ELE_TYPE).append(">").append("VARCHAR").append("</").append(ELE_TYPE).append(">");
			buffer.append("<").append(ELE_SIZE).append(">").append("42").append("</").append(ELE_SIZE).append(">");
			buffer.append("<").append(ELE_TABLE).append(">").append("").append("</").append(ELE_TABLE).append(">");
			buffer.append("</").append(ELE_COL).append(">");
		}

		ResourceEndpoint source = results.getSource();
		buffer.append("<").append(ELE_SOURCE).append(">");
		buffer.append("<").append(ELE_URL).append(">");
		buffer.append(ServletResponseWriter.normalize(source.getURL().toString()));
		buffer.append("</").append(ELE_URL).append(">");
		buffer.append("<").append(ELE_ID).append(">");
		buffer.append(source.getResourceID());
		buffer.append("</").append(ELE_ID).append(">");
		buffer.append("</").append(ELE_SOURCE).append(">");

		ResourceEndpoint target = results.getTarget();
		buffer.append("<").append(ELE_TARGET).append(">");
		buffer.append("<").append(ELE_URL).append(">");
		buffer.append(ServletResponseWriter.normalize(target.getURL().toString()));
		buffer.append("</").append(ELE_URL).append(">");
		buffer.append("<").append(ELE_ID).append(">");
		buffer.append(target.getResourceID());
		buffer.append("</").append(ELE_ID).append(">");
		buffer.append("</").append(ELE_TARGET).append(">");

		String query = results.getQuery();
		buffer.append("<").append(ELE_QUERY).append(">");
		buffer.append(ServletResponseWriter.normalize(query));
		buffer.append("</").append(ELE_QUERY).append(">");

		buffer.append("</").append(ELE_METADATA).append(">");

		for (String[] thisRow : data) {
			buffer.append("<").append(ELE_ROW).append(">");
			for (int i = 0; i < columnCount; i++) {
				String colValue = thisRow[i];
				if (colValue != null) {
					buffer.append("<").append(ELE_COL).append(">");
					buffer.append(ServletResponseWriter.normalize(colValue));
					buffer.append("</").append(ELE_COL).append(">");
				} else {
					buffer.append("<").append(ELE_COL).append(" ").append(ATTR_NULL).append("=\"" + VAL_TRUE + "\"").append("/>");
				}
			}
			buffer.append("</").append(ELE_ROW).append(">");
		}
		String warning = ts.getWarning();
		if (warning != null) {
			buffer.append("<").append(ELE_WARNING).append(">");
			buffer.append(ServletResponseWriter.normalize(warning));
			buffer.append("</").append(ELE_WARNING).append(">");
		}
		buffer.append("</").append(ELE_ROOT).append(">");
		return buffer.toString();
	}
	
}
