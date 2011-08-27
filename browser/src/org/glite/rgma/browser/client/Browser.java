package org.glite.rgma.browser.client;

import java.util.Set;
import java.util.TreeSet;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.Response;
import com.google.gwt.http.client.URL;
import com.google.gwt.user.client.ui.DecoratedTabPanel;
import com.google.gwt.user.client.ui.DockPanel;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.NodeList;
import com.google.gwt.xml.client.XMLParser;


public class Browser implements EntryPoint {

	/* To enable logging define LOG = true; This is useful for debugging. */
	private static final boolean LOG = false;
	
	private ListBox m_schemas;
	private ListBox m_tables;
	private Message m_message = new Message();
	private SchemaPanel m_schemaPanel;
	private QueryPanel m_queryPanel;
	private PublishPanel m_publishPanel;

	/**
	 * This is the entry point method.
	 */
	public void onModuleLoad() {

		final VerticalPanel mainPanel = new VerticalPanel();
		mainPanel.setWidth("100%");
		RootPanel.get().add(mainPanel);

		Log m_log = Log.getInstance();
		mainPanel.add(m_log);

		if (!LOG) {
			m_log.disable();
		}

		final DockPanel topPanel = new DockPanel();
		topPanel.setWidth("100%");
		topPanel.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
		mainPanel.add(topPanel);

		Label title = new Label("R-GMA Web Tool");
		title.addStyleName("MainTitle");
		topPanel.add(title, DockPanel.WEST);

		HorizontalPanel centrePanel = new HorizontalPanel();
		topPanel.add(centrePanel, DockPanel.CENTER);
		Label schemaLabel = new Label("Schema");
		centrePanel.add(schemaLabel);

		m_schemas = new ListBox();
		m_schemas.setVisibleItemCount(1);
		m_schemas.addStyleName("SmallLeft");
		centrePanel.add(m_schemas);

		Label tableLabel = new Label("Table");
		tableLabel.addStyleName("BigLeft");
		centrePanel.add(tableLabel);

		m_tables = new ListBox();
		m_tables.setVisibleItemCount(1);
		m_tables.addStyleName("SmallLeft");
		centrePanel.add(m_tables);
		getSchemas();
		m_schemas.addChangeHandler(new ChangeHandler() {
			public void onChange(ChangeEvent event) {
				getTables(m_schemas.getItemText(m_schemas.getSelectedIndex()));
			}
		});
		m_tables.addChangeHandler(new ChangeHandler() {
			public void onChange(ChangeEvent event) {
				getTableDef(m_tables.getItemText(m_tables.getSelectedIndex()));
			}
		});

		m_message.addStyleName("BigLeft");
		mainPanel.add(m_message);

		final DecoratedTabPanel tabPanel = new DecoratedTabPanel();
		tabPanel.setWidth("100%");
		mainPanel.add(tabPanel);

		m_schemaPanel = new SchemaPanel();
		tabPanel.add(m_schemaPanel, "Schema");

		m_queryPanel = new QueryPanel();
		tabPanel.add(m_queryPanel, "Query");

		m_publishPanel = new PublishPanel();

		tabPanel.add(m_publishPanel, "Publish");

		tabPanel.selectTab(1);

		m_log.add("Application started");
	}

	private void getSchemas() {
		final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode("/R-GMA/SchemaServlet/getProperty?name=resources"));
		try {
			m_message.clear();
			builder.sendRequest(null, new RequestCallback() {
				public void onError(Request request, Throwable exception) {
					m_message.display(exception);
				}

				public void onResponseReceived(Request request, Response response) {
					if (200 == response.getStatusCode()) {
						final Document messageDom = XMLParser.parse(response.getText());
						final NodeList vdbs = messageDom.getElementsByTagName("VDB");
						final int nVdb = vdbs.getLength();
						final Set<String> result = new TreeSet<String>();
						for (int i = 0; i < nVdb; i++) {
							result.add(vdbs.item(i).getAttributes().getNamedItem("ID").getNodeValue());
						}
						for (final String s : result) {
							m_schemas.addItem(s);
						}
						if (result.size() > 0) {
							getTables(m_schemas.getItemText(0));
						}
					} else {
						m_message.display(response);
					}
				}
			});
		} catch (final RequestException e) {
			m_message.display(e);
		}
	}

	private void getTableDef(String name) {
		final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode("/R-GMA/SchemaServlet/getTableDefinition?vdbName="
				+ m_schemas.getItemText(m_schemas.getSelectedIndex()) + "&tableName=" + name));
		m_message.clear();
		try {
			builder.sendRequest(null, new RequestCallback() {
				public void onError(Request request, Throwable exception) {
					m_message.display(exception);
				}

				public void onResponseReceived(Request request, Response response) {
					if (200 == response.getStatusCode()) {
						try {
							String schemaName = m_schemas.getItemText(m_schemas.getSelectedIndex());
							final TupleSet ts = XMLConverter.convertXMLResponseWithoutUnknownResource(response.getText());
							final String[] row = ts.getData().get(0);
							final String tableName = row[0];
							final String viewFor = row[6];
							m_schemaPanel.setTableName(schemaName, tableName, viewFor);
							m_queryPanel.setTableName(schemaName, tableName, viewFor);
							if (viewFor == null) {
								m_publishPanel.setTableName(schemaName, tableName);
							}
							for (final String[] t : ts.getData()) {
								String size = t[3];
								String type = size == "0" ? t[2] : t[2] + "(" + size + ")";
								m_queryPanel.addAttribute(t[1], t[2], Integer.parseInt(size));
								if (viewFor == null) {
									m_publishPanel.addAttribute(t[1], t[2], Integer.parseInt(size));
								}
								m_schemaPanel.addAttribute(t[1], type, Boolean.parseBoolean(t[5]), Boolean.parseBoolean(t[4]));
							}
						} catch (final RGMAException e) {
							m_message.display(e);
						}
					} else {
						m_message.display(response);
					}
				}
			});
		} catch (final RequestException e) {
			m_message.display(e);
		}
	}

	private void getTables(String vdbName) {
		final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode("/R-GMA/SchemaServlet/getAllTables?vdbName=" + vdbName));
		m_message.clear();
		try {
			builder.sendRequest(null, new RequestCallback() {
				public void onError(Request request, Throwable exception) {
					m_message.display(exception);
				}

				public void onResponseReceived(Request request, Response response) {
					if (200 == response.getStatusCode()) {
						try {
							final TupleSet ts = XMLConverter.convertXMLResponseWithoutUnknownResource(response.getText());
							m_tables.clear();
							for (final String[] t : ts.getData()) {
								m_tables.addItem(t[0]);
							}
							if (ts.getData().size() > 0) {
								getTableDef(ts.getData().get(0)[0]);
							}
						} catch (final RGMAException e) {
							m_message.display(e);
						}
					} else {
						m_message.display(response);
					}
				}
			});
		} catch (final RequestException e) {
			m_message.display(e);
		}
	}

}
