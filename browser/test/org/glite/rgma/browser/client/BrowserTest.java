package org.glite.rgma.browser.client;

import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;
import com.google.gwt.xml.client.NodeList;
import com.google.gwt.xml.client.XMLParser;

public class BrowserTest extends GWTTestCase {

	public void test1() throws Exception {
		/* Note the various spaces in the XML */
		final String td = " <r c = \"2\" r=\"2\">" + 
		" <v></v> <n/>" + 
		" <v>-1</v> <v>AnotherTable</v>" + 
		" </r>";

		TupleSet ts = XMLConverter.convertXMLResponseWithoutUnknownResource(td);

		assertEquals("Warning", "", ts.getWarning());
		assertEquals("EOR", false, ts.isEndOfResults());
		assertEquals("00", "", ts.getData().get(0)[0]);
		assertEquals("00", null, ts.getData().get(0)[1]);
		assertEquals("00", "-1", ts.getData().get(1)[0]);
		assertEquals("00", "AnotherTable", ts.getData().get(1)[1]);
	}

	public void test2() {
		final String xml = "<Schema ReplicationIntervalMillis=\"300000\">"
				+ "<VDB ID=\"registryTest\" MasterURL=\"https://rgma07.pp.rl.ac.uk:8443/R-GMA/SchemaServlet\" LastSuccessfulReplicationIntervalMillis=\"This is the master\"></VDB>"
				+ "<VDB ID=\"schemaTest\" MasterURL=\"https://rgma07.pp.rl.ac.uk:8443/R-GMA/SchemaServlet\" LastSuccessfulReplicationIntervalMillis=\"This is the master\"></VDB>"
				+ "<VDB ID=\"default\" MasterURL=\"https://rgma07.pp.rl.ac.uk:8443/R-GMA/SchemaServlet\" LastSuccessfulReplicationIntervalMillis=\"This is the master\"></VDB>"
				+ "</Schema>";

		final Document messageDom = XMLParser.parse(xml);
		final Element de = messageDom.getDocumentElement();
		final NodeList vdbs = de.getElementsByTagName("VDB");
		final int nVdb = vdbs.getLength();
		String[] expected = { "registryTest", "schemaTest", "default" };
		for (int i = 0; i < nVdb; i++) {
			final Node v = vdbs.item(i);
			assertEquals(expected[i], v.getAttributes().getNamedItem("ID").getNodeValue());
		}
	}

	public void test3() throws Exception {
		final String td = "<r c = \"7\" r=\"8\">" + "<v>userTable</v><v>userid</v><v>VARCHAR</v><v>255</v><v>true</v><v>true</v><n/>"
				+ "<v>userTable</v><v>astring</v><v>VARCHAR</v><v>255</v><v>false</v><v>false</v><n/>"
				+ "<v>userTable</v><v>areal</v><v>REAL</v><v>0</v><v>false</v><v>false</v><n/>"
				+ "<v>userTable</v><v>anint</v><v>INTEGER</v><v>0</v><v>false</v><v>false</v><n/>"
				+ "<v>userTable</v><v>RgmaTimestamp</v><v>TIMESTAMP</v><v>0</v><v>true</v><v>false</v><n/>"
				+ "<v>userTable</v><v>RgmaLRT</v><v>TIMESTAMP</v><v>0</v><v>true</v><v>false</v><n/>"
				+ "<v>userTable</v><v>RgmaOriginalServer</v><v>VARCHAR</v><v>255</v><v>true</v><v>false</v><n/>"
				+ "<v>userTable</v><v>RgmaOriginalClient</v><v>VARCHAR</v><v>255</v><v>true</v><v>false</v><n/>" + "</r>";

		TupleSet ts = XMLConverter.convertXMLResponseWithoutUnknownResource(td);

		assertEquals("Warning", "", ts.getWarning());
		assertEquals("EOR", false, ts.isEndOfResults());
		assertEquals("00", "userTable", ts.getData().get(0)[0]);
		assertEquals("11", "astring", ts.getData().get(1)[1]);
		assertEquals("22", "REAL", ts.getData().get(2)[2]);
		assertEquals("33", "0", ts.getData().get(3)[3]);
		assertEquals("76", null, ts.getData().get(7)[6]);
	}

	public void test4() throws Exception {
		final String td = "<p m=\"Required parameter &quot;isHistory&quot; not found\" o = \"2\"/>";
		try {
			XMLConverter.convertXMLResponseWithoutUnknownResource(td);
		} catch (RGMAPermanentException e) {
			assertEquals("o", 2, e.getNumSuccessfulOps());
			assertEquals("m", "Required parameter \"isHistory\" not found", e.getMessage());
			return;
		}
		fail("Exception not thrown");
	}

	public void test5() throws Exception {
		final String td = "<t m=\"Invalid format for identifier: 'TABLE USERTABLE'\"/>";
		try {
			XMLConverter.convertXMLResponseWithoutUnknownResource(td);
		} catch (RGMATemporaryException e) {
			assertEquals("o", 0, e.getNumSuccessfulOps());
			assertEquals("m", "Invalid format for identifier: 'TABLE USERTABLE'", e.getMessage());
			return;
		}
		fail("Exception not thrown");
	}

	public void test6() throws Exception {
		final String td = "<r r = \"0\" c = \"0\"/>";

		TupleSet ts = XMLConverter.convertXMLResponseWithoutUnknownResource(td);

		assertEquals("Warning", "", ts.getWarning());
		assertEquals("EOR", false, ts.isEndOfResults());
		try {
			ts.getData().get(0);
		} catch (IndexOutOfBoundsException e) {
			return;
		}
		fail("Exception not thrown");
	}

	public void test7() throws Exception {
		final String td = "<r r = \"0\" c = \"0\"><e/></r>";

		TupleSet ts = XMLConverter.convertXMLResponseWithoutUnknownResource(td);

		assertEquals("Warning", "", ts.getWarning());
		assertEquals("EOR", true, ts.isEndOfResults());
		try {
			ts.getData().get(0);
		} catch (IndexOutOfBoundsException e) {
			return;
		}
		fail("Exception not thrown");
	}

	public void test8() throws Exception {
		final String td = "<r r = \"0\" c = \"0\" m = \"Be warned\"><e/></r>";

		TupleSet ts = XMLConverter.convertXMLResponseWithoutUnknownResource(td);

		assertEquals("Warning", "Be warned", ts.getWarning());
		assertEquals("EOR", true, ts.isEndOfResults());
		try {
			ts.getData().get(0);
		} catch (IndexOutOfBoundsException e) {
			return;
		}
		fail("Exception not thrown");
	}

	public void test9() throws Exception {
		final String td = "<u/>";
		try {
			XMLConverter.convertXMLResponse(td);
		} catch (UnknownResourceException e) {
			assertEquals("m", "Unknown resource.", e.getMessage());
			return;
		}
		fail("Exception not thrown");
	}

	@Override
	public String getModuleName() {
		return "org.glite.rgma.browser.Browser";
	}
}
