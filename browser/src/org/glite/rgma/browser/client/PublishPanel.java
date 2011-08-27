package org.glite.rgma.browser.client;

import java.util.HashMap;
import java.util.Map;

import org.glite.rgma.browser.client.IntegerInput.IntegerInputException;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.Response;
import com.google.gwt.http.client.URL;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;

public class PublishPanel extends VerticalPanel {

	private final Label m_tableLabel;
	private final FlexTable m_rows;
	private boolean m_manualTimeStamp;
	private TextBox m_timeStampBox;
	private TextBox m_name;
	private Message m_message;
	private RadioButton m_named;
	private ListBox m_type;
	private RadioButton m_mem;
	private RadioButton m_db;
	private int m_resourceId;
	private TextBox m_predicate;
	private IntegerInput m_hrp;
	private IntegerInput m_lrp;

	class RBListener implements ClickHandler {
		public void onClick(ClickEvent event) {
			m_name.setVisible(m_named.getValue());
		}
	}

	private Map<String, TypeInfo> m_typeInfo = new HashMap<String, TypeInfo>();

	private class TypeInfo {
		public TypeInfo(String quoteValue, String description) {
			m_quoteValue = quoteValue;
			m_description = description;
		}

		private String m_quoteValue;
		private String m_description;
	}

	enum ATPos {
		NAME(0), TYPE(1), VALUE(2), EXTRA(3);

		private int m_offset;

		ATPos(int offset) {
			m_offset = offset;
		}

		public int offset() {
			return m_offset;
		}
	}

	PublishPanel() {
		String quote = "'";
		String noquote = "";
		m_typeInfo.put("VARCHAR", new TypeInfo(quote, "Unquoted string"));
		m_typeInfo.put("CHAR", new TypeInfo(quote, "Unquoted string"));
		m_typeInfo.put("DATE", new TypeInfo(quote, "YYYY-MM-DD"));
		m_typeInfo.put("TIME", new TypeInfo(quote, "HH:MM:SS"));
		m_typeInfo.put("TIMESTAMP", new TypeInfo(quote, "YYYY-MM-DD HH:MM:SS"));
		m_typeInfo.put("REAL", new TypeInfo(noquote, "Number"));
		m_typeInfo.put("INTEGER", new TypeInfo(noquote, "Number"));

		setWidth("100%");

		HorizontalPanel dbbuttons = new HorizontalPanel();
		dbbuttons.addStyleName(Style.VSPACE);
		dbbuttons.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
		add(dbbuttons);

		String storageTip = "Choose the kind of storage you want for your producer.";
		Label storageHeader = new Label("Storage:");
		storageHeader.setTitle(storageTip);
		storageHeader.addStyleName("RowHeader");
		RBListener rbl = new RBListener();
		m_mem = new RadioButton("MemOrDB", "Memory");
		m_mem.addClickHandler(rbl);
		m_mem.setValue(true);
		m_mem.setTitle(storageTip);
		m_db = new RadioButton("MemOrDB", "Database");
		m_db.addClickHandler(rbl);
		m_db.setTitle(storageTip);
		m_named = new RadioButton("MemOrDB", "Named database");
		m_named.addClickHandler(rbl);
		m_named.setTitle(storageTip);
		m_name = new TextBox();
		m_name.setTitle("Choose the name for re-usable data storage");

		dbbuttons.add(storageHeader);
		dbbuttons.add(m_mem);
		dbbuttons.add(m_db);
		dbbuttons.add(m_named);
		dbbuttons.add(m_name);
		m_name.setVisible(false);

		HorizontalPanel ptbuttons = new HorizontalPanel();
		ptbuttons.addStyleName(Style.VSPACE);
		ptbuttons.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
		add(ptbuttons);

		Label producerHeader = new Label("Producer:");
		producerHeader.addStyleName("RowHeader");
		ptbuttons.add(producerHeader);

		m_type = new ListBox();
		m_type.setVisibleItemCount(1);
		m_type.addItem("C", "isHistory=false&isLatest=false");
		m_type.addItem("CH", "isHistory=true&isLatest=false");
		m_type.addItem("CHL", "isHistory=true&isLatest=true");
		m_type.addItem("CL", "isHistory=false&isLatest=true");
		m_type.addStyleName(Style.SMALLLEFT);
		ptbuttons.add(m_type);

		Label predicateLabel = new Label("Predicate");
		predicateLabel.addStyleName(Style.BIGLEFT);
		ptbuttons.add(predicateLabel);
		m_predicate = new TextBox();
		m_predicate.addStyleName(Style.SMALLLEFT);
		m_predicate.setWidth("30em");
		ptbuttons.add(m_predicate);

		m_hrp = new IntegerInput("HRP", "secs");
		m_hrp.setValue(900);
		ptbuttons.add(m_hrp);

		m_lrp = new IntegerInput("LRP", "secs");
		m_lrp.setValue(900);
		ptbuttons.add(m_lrp);

		HorizontalPanel insertButtons = new HorizontalPanel();
		insertButtons.addStyleName(Style.VSPACE);
		insertButtons.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
		add(insertButtons);

		Label insertHeader = new Label("Insert into");
		insertHeader.addStyleName(Style.ROWHEADER);
		insertButtons.add(insertHeader);
		m_tableLabel = new Label();
		m_tableLabel.addStyleName(Style.SMALLLEFT);
		insertButtons.add(m_tableLabel);

		m_rows = new FlexTable();
		m_rows.getRowFormatter().addStyleName(0, Style.COLUMNHEADER);
		m_rows.setText(0, ATPos.NAME.offset(), "Name");
		m_rows.setText(0, ATPos.TYPE.offset(), "Type");
		m_rows.setText(0, ATPos.VALUE.offset(), "Value");
		add(m_rows);

		HorizontalPanel pubButtons = new HorizontalPanel();
		pubButtons.addStyleName(Style.VSPACE);
		pubButtons.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
		add(pubButtons);

		final Button b = new Button("Publish");
		b.addClickHandler(new ClickHandler() {
			public void onClick(ClickEvent event) {
				StringBuilder sb = new StringBuilder("/R-GMA/PrimaryProducerServlet/createPrimaryProducer");
				sb.append("?type=" + (m_mem.getValue() ? "memory" : "database"));
				if (m_named.getValue()) {
					sb.append("&logicalName=" + m_name.getText());
				}
				sb.append("&" + m_type.getValue(m_type.getSelectedIndex()));
				RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode(sb.toString()));
				try {
					builder.sendRequest(null, new RequestCallback() {
						public void onError(Request request, Throwable exception) {
							m_message.display(exception);
						}

						public void onResponseReceived(Request request, Response response) {
							if (200 == response.getStatusCode()) {
								TupleSet ts;
								try {
									ts = XMLConverter.convertXMLResponseWithoutUnknownResource(response.getText());
									m_resourceId = Integer.parseInt(ts.getData().get(0)[0]);
									m_message.display("Resource " + m_resourceId + " created");
									declareTable();
								} catch (Exception e) {
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
		});
		pubButtons.add(b);

		m_message = new Message();
		m_message.addStyleName(Style.BIGLEFT);
		pubButtons.add(m_message);

	}

	private void declareTable() throws RequestException {

		StringBuilder sb = new StringBuilder("/R-GMA/PrimaryProducerServlet/declareTable?connectionId=" + m_resourceId);
		sb.append("&tableName=" + m_tableLabel.getText());
		sb.append("&predicate=" + m_predicate.getText());
		
		try {
			sb.append("&hrpSec=" + (m_hrp.isBlank() ? 0 : m_hrp.getValue()));
			sb.append("&lrpSec=" + (m_lrp.isBlank() ? 0 : m_lrp.getValue()));
		} catch (IntegerInputException e) {
			m_message.display(e);
			return;
		}
		
		RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode(sb.toString()));

		builder.sendRequest(null, new RequestCallback() {
			public void onError(Request request, Throwable exception) {
				m_message.display(exception);
			}

			public void onResponseReceived(Request request, Response response) {
				if (200 == response.getStatusCode()) {
					try {
						final TupleSet ts = XMLConverter.convertXMLResponse(response.getText());
						if (!ts.getData().get(0)[0].equals("OK")) {
							throw new RGMAPermanentException("Internal error");
						}
						m_message.display("Table declared");
						insertTuple();
					} catch (Exception e) {
						m_message.display(e);
					}
				} else {
					m_message.display(response);
				}
			}
		});

	}

	private void insertTuple() throws RequestException {

		try {
			StringBuffer names = new StringBuffer();
			StringBuffer values = new StringBuffer();
			int nmax = m_rows.getRowCount();
			for (int n = 1; n < nmax; n++) {
				String name = m_rows.getText(n, ATPos.NAME.offset());
				if (name.equals("RgmaTimestamp")) {
					if (!m_manualTimeStamp) {
						continue;
					}
				}
				if (names.length() != 0) {
					names.append(',');
					values.append(',');
				}
				names.append(name);
				String quote = m_typeInfo.get((m_rows.getText(n, ATPos.TYPE.offset()))).m_quoteValue;
				values.append(quote + ((TextBox) m_rows.getWidget(n, ATPos.VALUE.offset())).getText() + quote);
			}

			StringBuilder sb = new StringBuilder("/R-GMA/PrimaryProducerServlet/insert?connectionId=" + m_resourceId);
			sb.append("&insert=" + "INSERT INTO " + m_tableLabel.getText() + "(").append(names).append(") VALUES (").append(values).append(')');
			RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode(sb.toString()));

			builder.sendRequest(null, new RequestCallback() {
				public void onError(Request request, Throwable exception) {
					m_message.display(exception);
				}

				public void onResponseReceived(Request request, Response response) {
					if (200 == response.getStatusCode()) {
						try {
							final TupleSet ts = XMLConverter.convertXMLResponse(response.getText());
							if (!ts.getData().get(0)[0].equals("OK")) {
								throw new RGMAPermanentException("Internal error");
							}
							m_message.display("Data published");
						} catch (Exception e) {
							m_message.display(e);
						}
					} else {
						m_message.display(response);
					}
				}

			});
		} catch (Exception e) {
			m_message.display(e);
		}

	}

	void addAttribute(String name, String type, int size) {
		if (name.startsWith("Rgma") && !name.equals("RgmaTimestamp")) {
			return;
		}
		final int n = m_rows.getRowCount();
		m_rows.setText(n, ATPos.NAME.offset(), name);
		m_rows.setText(n, ATPos.TYPE.offset(), type);

		final TextBox tb = new TextBox();
		if (size != 0) {
			tb.setWidth(Math.min(size, 30) + "em");
		}
		tb.setTitle(m_typeInfo.get(type).m_description);
		if (name.equals("RgmaTimestamp")) {
			m_timeStampBox = tb;
			if (!m_manualTimeStamp) {
				tb.setEnabled(false);
			}
			final CheckBox cb = new CheckBox("Manually set timestamp field");
			cb.addClickHandler(new ClickHandler() {
				public void onClick(ClickEvent event) {
					m_manualTimeStamp = cb.getValue();
					if (m_timeStampBox != null) {
						m_timeStampBox.setEnabled(m_manualTimeStamp ? true : false);
					}
				}
			});
			m_rows.setWidget(n, ATPos.EXTRA.offset(), cb);
		}
		m_rows.setWidget(n, ATPos.VALUE.offset(), tb);
	}

	public void setTableName(String vdbName, String tableName) {
		m_tableLabel.setText(vdbName + "." + tableName);
		int nmax = m_rows.getRowCount();
		for (int n = 1; n < nmax; n++) {
			m_rows.removeRow(1);
		}
	}
}
