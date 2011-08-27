package org.glite.rgma.browser.client;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.glite.rgma.browser.client.IntegerInput.IntegerInputException;

import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.Response;
import com.google.gwt.http.client.URL;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;

public class QueryPanel extends VerticalPanel {

	private class ProducerTableEntry {

		final private String m_url;
		final private int m_connectionId;
		final private int m_hrpSec;

		public ProducerTableEntry(String url, int connectionId, int hrpSec) {
			m_url = url;
			m_connectionId = connectionId;
			m_hrpSec = hrpSec;
		}
	}

	enum ATPos {
		NAME(0), TYPE(1), SHOW(2), OP(3), VALUE(4);

		private int m_offset;

		ATPos(int offset) {
			m_offset = offset;
		}

		public int offset() {
			return m_offset;
		}
	}

	enum PType {
		PRIMARY(0), SECONDARY(1), ONDEMAND(2);

		private int m_offset;

		PType(int offset) {
			m_offset = offset;
		}

		public int offset() {
			return m_offset;
		}
	}

	enum QType {
		CONTINUOUS(0), HISTORY(1), LATEST(2), STATIC(3);

		private int m_offset;

		QType(int offset) {
			m_offset = offset;
		}

		public int offset() {
			return m_offset;
		}
	}

	private static final int POPSLEEP = 1000;
	private static final int BIGPOPSLEEP = 20000;
	private static final int DISPLAYSLEEP = 500;

	private FlexTable m_tableAtts;
	private Label m_tableLabel;
	private String m_vdbName;
	private String m_tableName;
	private final Message m_message;
	private FlexTable m_smallTable;
	private ListBox m_type;
	private CheckBox m_useMediator;
	private Integer m_resourceId;
	private boolean m_freeRunning;

	private final List<List<List<ProducerTableEntry>>> m_ptelArray = new ArrayList<List<List<ProducerTableEntry>>>(4);

	private ProducerList[] m_producerList = new ProducerList[3];
	private IntegerInput m_queryIntervalSecs;
	private Label m_queryIntervalLabel;
	private ListBox m_bufferSize;
	private FlexTable m_resultsTable;
	private Button[] m_next = new Button[2];
	private Label m_warning = new Label();
	private List<String[]> m_data = new LinkedList<String[]>();
	private Timer m_popTimer;
	private Timer m_displayTimer;

	private Log m_log = Log.getInstance();
	private Button[] m_continue = new Button[2];
	private Button[] m_pause = new Button[2];
	private int m_tupleNum;
	private Button m_refresh;
	private Button[] m_stop = new Button[2];
	private Button[] m_run = new Button[2];

	QueryPanel() {

		setWidth("100%");

		for (int i = 0; i < 4; i++) {
			m_ptelArray.add(null);
		}

		ArrayList<List<ProducerTableEntry>> p = new ArrayList<List<ProducerTableEntry>>(3);
		for (int i = 0; i < 3; i++) {
			p.add(null);
		}
		p.set(PType.PRIMARY.offset(), new ArrayList<ProducerTableEntry>());
		p.set(PType.SECONDARY.offset(), new ArrayList<ProducerTableEntry>());
		m_ptelArray.set(QType.CONTINUOUS.offset(), p);

		p = new ArrayList<List<ProducerTableEntry>>(3);
		for (int i = 0; i < 3; i++) {
			p.add(null);
		}
		p.set(PType.PRIMARY.offset(), new ArrayList<ProducerTableEntry>());
		p.set(PType.SECONDARY.offset(), new ArrayList<ProducerTableEntry>());
		m_ptelArray.set(QType.HISTORY.offset(), p);

		p = new ArrayList<List<ProducerTableEntry>>(3);
		for (int i = 0; i < 3; i++) {
			p.add(null);
		}
		p.set(PType.PRIMARY.offset(), new ArrayList<ProducerTableEntry>());
		p.set(PType.SECONDARY.offset(), new ArrayList<ProducerTableEntry>());
		m_ptelArray.set(QType.LATEST.offset(), p);

		p = new ArrayList<List<ProducerTableEntry>>(3);
		for (int i = 0; i < 3; i++) {
			p.add(null);
		}
		p.set(PType.ONDEMAND.offset(), new ArrayList<ProducerTableEntry>());
		m_ptelArray.set(QType.STATIC.offset(), p);

		HorizontalPanel tableNameButtons = new HorizontalPanel();
		tableNameButtons.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
		add(tableNameButtons);

		Label tableNameLabel = new Label("Select from");
		tableNameLabel.addStyleName(Style.ROWHEADER);
		tableNameButtons.add(tableNameLabel);
		m_tableLabel = new Label();
		m_tableLabel.addStyleName(Style.SMALLLEFT);
		tableNameButtons.add(m_tableLabel);

		m_tableAtts = new FlexTable();
		m_tableAtts.addStyleName(Style.VSPACE);
		m_tableAtts.getRowFormatter().addStyleName(0, "ColumnHeader");
		m_tableAtts.setText(0, ATPos.NAME.offset(), "Name");
		m_tableAtts.setText(0, ATPos.TYPE.offset(), "Type");
		m_tableAtts.setText(0, ATPos.SHOW.offset(), "Show");
		m_tableAtts.setText(0, ATPos.OP.offset(), "Condition");
		m_tableAtts.setText(0, ATPos.VALUE.offset(), "Value");
		add(m_tableAtts);

		m_smallTable = new FlexTable();
		m_smallTable.addStyleName(Style.DATATABLE);
		add(m_smallTable);
		m_smallTable.getRowFormatter().addStyleName(0, Style.COLUMNHEADER);
		for (int j = 1; j < 5; j++) {
			m_smallTable.getCellFormatter().addStyleName(0, j, Style.DATA);
		}
		for (int n = 1; n <= 3; n++) {
			m_smallTable.getCellFormatter().addStyleName(n, 0, Style.ROWHEADER);
			for (int j = 0; j < 5; j++) {
				m_smallTable.getCellFormatter().addStyleName(n, j, Style.DATA);
			}
		}
		HorizontalPanel hp = new HorizontalPanel();
		hp.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
		hp.addStyleName(Style.VSPACE);
		add(hp);
		m_type = new ListBox();
		m_type.setVisibleItemCount(1);
		m_type.addChangeHandler(new ChangeHandler() {
			public void onChange(ChangeEvent event) {
				displayRelevantProducers();
			}
		});
		hp.add(m_type);
		m_useMediator = new CheckBox("Use Mediator");
		m_useMediator.addClickHandler(new ClickHandler() {
			public void onClick(ClickEvent event) {
				displayRelevantProducers();
			}
		});
		m_useMediator.setValue(true);
		m_useMediator.addStyleName(Style.BIGLEFT);
		m_useMediator.setTitle("If not checked you can choose your own producers to answer the query.");
		hp.add(m_useMediator);

		m_refresh = new Button("Refresh");
		m_refresh.addStyleName(Style.BIGLEFT);
		m_refresh.setTitle("Get current information from the registry. This is done auotomatically when you change table.");
		m_refresh.addClickHandler(new ClickHandler() {
			public void onClick(ClickEvent event) {
				getAllProducersForTable();
			}
		});
		hp.add(m_refresh);

		m_queryIntervalSecs = new IntegerInput("Query Interval", "secs");
		m_queryIntervalSecs.setTitle("Specify a query interval in seconds to say how far back in time you want to use tuples. "
				+ "If you want to issue a query with no interval leave this box blank. "
				+ "The default for continuous queries is 0 to only see new tuples, otherwise it is infinite to see all tuples.");
		hp.add(m_queryIntervalSecs);

		m_producerList[0] = new ProducerList("Primary", true);
		add(m_producerList[0]);
		m_producerList[1] = new ProducerList("Secondary", true);
		add(m_producerList[1]);
		m_producerList[2] = new ProducerList("OnDemand", false);
		add(m_producerList[2]);

		HorizontalPanel[] runBars = new HorizontalPanel[2];
		runBars[0] = new HorizontalPanel();
		runBars[0].setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);

		runBars[1] = new HorizontalPanel();
		runBars[1].setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);

		m_bufferSize = new ListBox();
		int[] values = { 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000 };
		for (int val : values) {
			m_bufferSize.addItem(String.valueOf(val));
		}
		m_bufferSize.setSelectedIndex(4);
		m_bufferSize.setTitle("Number of query results to display at once");
		Label label = new Label("Buffer Size");
		label.addStyleName("BigLeft");
		runBars[0].add(label);
		runBars[0].add(m_bufferSize);

		for (int i = 0; i < 2; i++) {
			HorizontalPanel rb = runBars[i];

			m_run[i] = new Button("Run");
			m_run[i].addStyleName(Style.BIGLEFT);
			m_run[i].setTitle("Excecute the query");
			m_run[i].addClickHandler(new ClickHandler() {
				public void onClick(ClickEvent event) {
					executeQuery();
				}
			});
			rb.add(m_run[i]);

			m_next[i] = new Button("Next");
			m_next[i].addStyleName(Style.BIGLEFT);
			m_next[i].setTitle("Discard displayed tuples and display next batch");
			m_next[i].addClickHandler(new ClickHandler() {
				public void onClick(ClickEvent event) {
					next();
				}
			});
			rb.add(m_next[i]);

			m_continue[i] = new Button("Continue");
			m_continue[i].addStyleName(Style.BIGLEFT);
			m_continue[i].setTitle("Continuously display new tuples while discarding the oldest ones");
			m_continue[i].addClickHandler(new ClickHandler() {
				public void onClick(ClickEvent event) {
					cont();
				}
			});
			rb.add(m_continue[i]);

			m_pause[i] = new Button("Pause");
			m_pause[i].addStyleName(Style.BIGLEFT);
			m_pause[i].setTitle("Freeze the display");
			m_pause[i].addClickHandler(new ClickHandler() {
				public void onClick(ClickEvent event) {
					pause();
				}
			});
			rb.add(m_pause[i]);

			m_stop[i] = new Button("Stop");
			m_stop[i].addStyleName(Style.BIGLEFT);
			m_stop[i].setTitle("Terminate the query");
			m_stop[i].addClickHandler(new ClickHandler() {
				public void onClick(ClickEvent event) {
					stop();
					m_displayTimer.cancel();
				}
			});
			rb.add(m_stop[i]);
		}
		m_message = new Message();
		m_message.addStyleName(Style.BIGLEFT);
		runBars[1].add(m_message);

		enableButtons("");

		add(runBars[0]);
		m_resultsTable = new FlexTable();
		m_resultsTable.addStyleName(Style.DATATABLE);
		add(m_resultsTable);

		m_warning.addStyleName(Style.BIGLEFT);
		add(m_warning);
		add(runBars[1]);

		m_popTimer = new Timer() {
			public void run() {
				pop();
			}
		};

		m_displayTimer = new Timer() {
			public void run() {
				display();
			}
		};
	}

	private void cont() {
		enableButtons("ps");
		m_freeRunning = true;
		m_popTimer.cancel();
		pop();
		m_displayTimer.cancel();
		display();
	}

	private void display() {
		try {
			int interval;
			String logMsg;
			if (m_freeRunning) {
				int needed = Integer.parseInt(m_bufferSize.getValue(m_bufferSize.getSelectedIndex()));
				int transfer = Math.min(m_data.size(), needed);
				logMsg = "Free display sees table size = " + (m_resultsTable.getRowCount() - 1) + ", bs = "
						+ m_bufferSize.getValue(m_bufferSize.getSelectedIndex()) + ", popped = " + m_data.size() + ", needed = " + needed + ", transfer = "
						+ transfer;
				for (int m = 0; m < transfer; m++) {
					int n = m_resultsTable.getRowCount();
					int ntogo = n - Integer.parseInt(m_bufferSize.getValue(m_bufferSize.getSelectedIndex()));
					if (ntogo > 0) {
						for (int k = 0; k < ntogo; k++) {
							m_resultsTable.removeRow(1);
						}
						n -= ntogo;
					}
					m_resultsTable.setText(n, 0, String.valueOf(m_tupleNum++));
					m_resultsTable.getCellFormatter().addStyleName(n, 0, Style.DATA);
					String[] t = m_data.remove(0);
					for (int colNum = 1; colNum < m_resultsTable.getCellCount(0); colNum++) {
						m_resultsTable.setText(n, colNum, t[colNum - 1]);
						m_resultsTable.getCellFormatter().addStyleName(n, colNum, Style.DATA);
					}
				}
				interval = (transfer == 0) ? DISPLAYSLEEP : 1;
			} else {
				int n = m_resultsTable.getRowCount();
				int needed = Integer.parseInt(m_bufferSize.getValue(m_bufferSize.getSelectedIndex())) - n + 1;
				if (needed <= 0) {
					needed = 0;
				}
				int transfer = Math.min(m_data.size(), needed);
				logMsg = "Next display sees table size = " + (m_resultsTable.getRowCount() - 1) + ", bs = "
						+ Integer.parseInt(m_bufferSize.getValue(m_bufferSize.getSelectedIndex())) + ", popped = " + m_data.size() + ", needed = " + needed
						+ ", transfer = " + transfer;
				for (int m = 0; m < transfer; m++) {
					m_resultsTable.setText(n, 0, String.valueOf(m_tupleNum++));
					m_resultsTable.getCellFormatter().addStyleName(n, 0, Style.DATA);
					String[] t = m_data.remove(0);
					for (int colNum = 1; colNum < m_resultsTable.getCellCount(0); colNum++) {
						m_resultsTable.setText(n, colNum, t[colNum - 1]);
						m_resultsTable.getCellFormatter().addStyleName(n, colNum, Style.DATA);
					}
					n++;
				}
				if (Integer.parseInt(m_bufferSize.getValue(m_bufferSize.getSelectedIndex())) - m_resultsTable.getRowCount() + 1 <= 0) {
					enableButtons("ncs");
				}
				interval = DISPLAYSLEEP;
			}
			m_log.add(logMsg + " scheduling display after " + interval + " ms");
			m_displayTimer.schedule(interval);
		} catch (Exception e) {
			m_message.display(e);
		}
	}

	private void displayRelevantProducers() {
		int index = m_type.getSelectedIndex();
		if (m_useMediator.getValue() || index == -1) {
			for (ProducerList pl : m_producerList) {
				pl.setVisible(false);
			}
			return;
		}
		QType type = QType.valueOf(m_type.getValue(index));
		List<List<ProducerTableEntry>> pteList = m_ptelArray.get(type.offset());
		for (int t = 0; t < pteList.size(); t++) {
			List<ProducerTableEntry> ptes = pteList.get(t);
			if (ptes != null && ptes.size() != 0) {
				ProducerList pl = m_producerList[t];
				pl.clear();
				for (ProducerTableEntry pte : ptes) {
					pl.add(pte.m_url, pte.m_connectionId, pte.m_hrpSec);
				}
				pl.setVisible(true);
			}
		}
		boolean show = type != QType.STATIC;
		m_queryIntervalLabel.setVisible(show);
		m_queryIntervalSecs.setVisible(show);
	}

	private void enableButtons(String buttonString) {
		for (int i = 0; i < 2; i++) {
			m_pause[i].setEnabled(buttonString.contains("p"));
			m_continue[i].setEnabled(buttonString.contains("c"));
			m_next[i].setEnabled(buttonString.contains("n"));
			m_stop[i].setEnabled(buttonString.contains("s"));
		}
	}

	private void executeQuery() {
		try {
			int index = m_type.getSelectedIndex();
			if (index == -1) {
				m_message.display("No producers available - try the refresh button");
				return;
			}
			setQueryControlsEnabled(false);
			enableButtons("cs");
			m_freeRunning = false;
			m_run[0].setText("Restart Query");
			m_run[1].setText("Restart Query");
			m_tupleNum = 0;
			if (m_resourceId != null) {
				final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode("/R-GMA/ConsumerServlet/close?connectionId=" + m_resourceId));
				try {
					builder.sendRequest(null, new RequestCallback() {

						public void onError(Request request, Throwable exception) {
							m_message.display(exception);
						}

						public void onResponseReceived(Request request, Response response) {}
					});
				} catch (final RequestException e) {
					m_message.display(e);
				}
			}
			StringBuilder sb = new StringBuilder("/R-GMA/ConsumerServlet/createConsumer?select=SELECT ");
			int nmax = m_tableAtts.getRowCount();
			boolean first = true;
			while (m_resultsTable.getRowCount() > 0) {
				m_resultsTable.removeRow(0);
			}
			m_resultsTable.setText(0, 0, "#");
			m_resultsTable.getCellFormatter().addStyleName(0, 0, Style.DATA);
			int colNum = 1;
			for (int n = 1; n < nmax; n++) {
				CheckBox cb = (CheckBox) m_tableAtts.getWidget(n, ATPos.SHOW.offset());
				if (cb.getValue()) {
					if (first) {
						first = false;
					} else {
						sb.append(',');
					}
					String colName = m_tableAtts.getText(n, ATPos.NAME.offset());
					sb.append(colName);
					m_resultsTable.setText(0, colNum, colName);
					m_resultsTable.getCellFormatter().addStyleName(0, colNum++, Style.DATA);
				}
			}
			m_resultsTable.getRowFormatter().addStyleName(0, "ColumnHeader");
			sb.append(" FROM " + m_vdbName + "." + m_tableName);
			first = true;
			for (int n = 1; n < nmax; n++) {
				ListBox lb = (ListBox) m_tableAtts.getWidget(n, ATPos.OP.offset());
				String value = lb.getValue(lb.getSelectedIndex());
				if (value.length() > 0) {
					if (first) {
						first = false;
						sb.append(" WHERE");
					} else {
						sb.append(" AND");
					}
					TextBox tb = (TextBox) m_tableAtts.getWidget(n, ATPos.VALUE.offset());
					String type = m_tableAtts.getText(n, ATPos.TYPE.offset());
					String quote = (type.equals("CHAR") || type.equals("VARCHAR") || type.equals("DATE") || type.equals("TIME") || type.equals("TIMESTAMP")) ? "'"
							: "";
					sb.append(" " + m_tableAtts.getText(n, ATPos.NAME.offset()) + " " + value + " " + quote + tb.getText() + quote);
				}
			}
			QType type = QType.valueOf(m_type.getValue(index));
			sb.append("&queryType=" + type.name().toLowerCase());
			if (!m_queryIntervalSecs.isBlank()) {
				sb.append("&timeIntervalSec=" + String.valueOf(m_queryIntervalSecs.getValue()));
			}
			if (!m_useMediator.getValue()) {
				m_producerList[0].appendListString(sb);
				m_producerList[1].appendListString(sb);
				m_producerList[2].appendListString(sb);
			}
			RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode(sb.toString()));
			try {
				builder.sendRequest(null, new RequestCallback() {
					public void onError(Request request, Throwable exception) {
						m_message.display(exception);
					}

					public void onResponseReceived(Request request, Response response) {
						if (200 == response.getStatusCode()) {
							try {
								TupleSet ts = XMLConverter.convertXMLResponseWithoutUnknownResource(response.getText());
								m_resourceId = Integer.parseInt(ts.getData().get(0)[0]);
								m_message.display("Resource " + m_resourceId + " created");
								m_data.clear();
								m_popTimer.cancel();
								pop();
								m_displayTimer.cancel();
								display();
							} catch (final RGMAException e) {
								m_message.display(e);
							}
						} else {
							m_message.display(response);
						}
					}
				});
			} catch (final RequestException e) {
				stop();
				m_message.display(e);
			}
		} catch (IntegerInputException e) {
			stop();
			m_message.display(e);
		} catch (Exception e) {
			stop();
			m_message.display(e);
		}
	}

	private void getAllProducersForTable() {

		final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL
				.encode("/R-GMA/RegistryServlet/getAllProducersForTable?canForward=True&vdbName=" + m_vdbName + "&tableName=" + m_tableName));

		try {
			builder.sendRequest(null, new RequestCallback() {
				public void onError(Request request, Throwable exception) {
					m_message.display(exception);
				}

				public void onResponseReceived(Request request, Response response) {
					if (200 == response.getStatusCode()) {
						try {
							List<String[]> ts = XMLConverter.convertXMLResponseWithoutUnknownResource(response.getText()).getData();

							m_ptelArray.get(QType.CONTINUOUS.offset()).get(PType.PRIMARY.offset()).clear();
							m_ptelArray.get(QType.CONTINUOUS.offset()).get(PType.SECONDARY.offset()).clear();
							m_ptelArray.get(QType.HISTORY.offset()).get(PType.PRIMARY.offset()).clear();
							m_ptelArray.get(QType.HISTORY.offset()).get(PType.SECONDARY.offset()).clear();
							m_ptelArray.get(QType.LATEST.offset()).get(PType.PRIMARY.offset()).clear();
							m_ptelArray.get(QType.LATEST.offset()).get(PType.SECONDARY.offset()).clear();
							m_ptelArray.get(QType.STATIC.offset()).get(PType.ONDEMAND.offset()).clear();

							for (final String[] t : ts) {
								final boolean isSecondaryProducer = Boolean.parseBoolean(t[2]);
								final boolean isContinuous = Boolean.parseBoolean(t[3]);
								final boolean isStatic = Boolean.parseBoolean(t[4]);
								final boolean isHistory = Boolean.parseBoolean(t[5]);
								final boolean isLatest = Boolean.parseBoolean(t[6]);
								final ProducerTableEntry pte = new ProducerTableEntry(t[0], Integer.parseInt(t[1]), Integer.parseInt(t[8]));
								if (isStatic) {
									m_ptelArray.get(QType.STATIC.offset()).get(PType.ONDEMAND.offset()).add(pte);
								} else {
									int pors = isSecondaryProducer ? PType.SECONDARY.offset() : PType.PRIMARY.offset();
									if (isContinuous) {
										m_ptelArray.get(QType.CONTINUOUS.offset()).get(pors).add(pte);
									}
									if (isHistory) {
										m_ptelArray.get(QType.HISTORY.offset()).get(pors).add(pte);
									}
									if (isLatest) {
										m_ptelArray.get(QType.LATEST.offset()).get(pors).add(pte);
									}
								}
							}
							m_smallTable.setText(0, QType.CONTINUOUS.offset() + 1, "Continuous");
							m_smallTable.setText(0, QType.HISTORY.offset() + 1, "History");
							m_smallTable.setText(0, QType.LATEST.offset() + 1, "Latest");
							m_smallTable.setText(0, 4, "Static");
							m_smallTable.setText(PType.PRIMARY.offset() + 1, 0, "Primary");
							m_smallTable.setText(PType.PRIMARY.offset() + 1, QType.CONTINUOUS.offset() + 1, Integer.toString(m_ptelArray.get(
									QType.CONTINUOUS.offset()).get(PType.PRIMARY.offset()).size()));
							m_smallTable.setText(PType.PRIMARY.offset() + 1, QType.HISTORY.offset() + 1, Integer.toString(m_ptelArray.get(
									QType.HISTORY.offset()).get(PType.PRIMARY.offset()).size()));
							m_smallTable.setText(PType.PRIMARY.offset() + 1, QType.LATEST.offset() + 1, Integer.toString(m_ptelArray.get(QType.LATEST.offset())
									.get(PType.PRIMARY.offset()).size()));
							m_smallTable.setText(PType.SECONDARY.offset() + 1, 0, "Secondary");
							m_smallTable.setText(PType.SECONDARY.offset() + 1, QType.CONTINUOUS.offset() + 1, Integer.toString(m_ptelArray.get(
									QType.CONTINUOUS.offset()).get(PType.SECONDARY.offset()).size()));
							m_smallTable.setText(PType.SECONDARY.offset() + 1, QType.HISTORY.offset() + 1, Integer.toString(m_ptelArray.get(
									QType.HISTORY.offset()).get(PType.SECONDARY.offset()).size()));
							m_smallTable.setText(PType.SECONDARY.offset() + 1, QType.LATEST.offset() + 1, Integer.toString(m_ptelArray.get(
									QType.LATEST.offset()).get(PType.SECONDARY.offset()).size()));
							m_smallTable.setText(PType.ONDEMAND.offset() + 1, 0, "OnDemand");
							m_smallTable.setText(PType.ONDEMAND.offset() + 1, QType.STATIC.offset() + 1, Integer.toString(m_ptelArray
									.get(QType.STATIC.offset()).get(PType.ONDEMAND.offset()).size()));
							m_type.clear();
							if (m_ptelArray.get(QType.CONTINUOUS.offset()).get(PType.PRIMARY.offset()).size()
									+ m_ptelArray.get(QType.CONTINUOUS.offset()).get(PType.SECONDARY.offset()).size() > 0) {
								m_type.addItem("C", QType.CONTINUOUS.name());
							}
							if (m_ptelArray.get(QType.HISTORY.offset()).get(PType.PRIMARY.offset()).size()
									+ m_ptelArray.get(QType.HISTORY.offset()).get(PType.SECONDARY.offset()).size() > 0) {
								m_type.addItem("H", QType.HISTORY.name());
							}
							if (m_ptelArray.get(QType.LATEST.offset()).get(PType.PRIMARY.offset()).size()
									+ m_ptelArray.get(QType.LATEST.offset()).get(PType.SECONDARY.offset()).size() > 0) {
								m_type.addItem("L", QType.LATEST.name());
							}
							if (m_ptelArray.get(QType.STATIC.offset()).get(PType.ONDEMAND.offset()).size() > 0) {
								m_type.addItem("S", QType.STATIC.name());
							}
							m_message.display("Available producers updated");
							displayRelevantProducers();

						} catch (final RGMAException e) {
							m_log.add(e.getMessage());
							m_message.display(e);
						}
					} else {
						m_message.display(response);
					}
				}
			});
		} catch (final RequestException e) {
			m_log.add(e.getMessage());
			m_message.display(e);
		}

	}

	private void next() {
		enableButtons("cs");
		m_freeRunning = false;
		while (m_resultsTable.getRowCount() > 1) {
			m_resultsTable.removeRow(1);
		}
		m_popTimer.cancel();
		pop();
		m_displayTimer.cancel();
		display();
	}

	private void pause() {
		enableButtons("cns");
		m_displayTimer.cancel();
	}

	private void pop() {
		try {
			int maxCount = Integer.parseInt(m_bufferSize.getValue(m_bufferSize.getSelectedIndex())) * 2 - m_data.size();
			if (maxCount > 0) {
				final RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode("/R-GMA/ConsumerServlet/pop?connectionId=" + m_resourceId
						+ "&maxCount=" + maxCount));
				try {
					builder.sendRequest(null, new RequestCallback() {

						public void onError(Request request, Throwable exception) {
							m_message.display(exception);
						}

						public void onResponseReceived(Request request, Response response) {
							if (200 == response.getStatusCode()) {
								try {
									final TupleSet ts = XMLConverter.convertXMLResponse(response.getText());
									String warning = ts.getWarning();
									if (warning != null) {
										m_warning.setText(warning);
									} else {
										m_warning.setText("");
									}
									List<String[]> data = ts.getData();
									m_data.addAll(data);
									if (ts.isEndOfResults()) {
										stop();
										return;
									}
									int interval = (data.size() == 0) ? POPSLEEP : 1;
									m_log.add("Popped " + data.size() + " tuples, eod = " + ts.isEndOfResults() + ", warning = " + ts.getWarning()
											+ " scheduling pop after " + interval + " ms");
									m_popTimer.schedule(interval);
								} catch (final RGMAException e) {
									m_message.display(e);
								} catch (UnknownResourceException e) {
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
			} else {
				if (!m_freeRunning && m_resultsTable.getRowCount() > Integer.parseInt(m_bufferSize.getValue(m_bufferSize.getSelectedIndex()))) {
					m_popTimer.cancel();
					m_log.add("pop will not be rescheduled");
					return;
				}
				int interval = m_freeRunning ? POPSLEEP : BIGPOPSLEEP;
				m_log.add("Buffer full so scheduling pop after " + interval + " ms");
				m_popTimer.schedule(interval);
			}
		} catch (Exception e) {
			m_message.display(e);
		}
	}

	private void setQueryControlsEnabled(boolean enabled) {
		int nmax = m_tableAtts.getRowCount();
		for (int n = 1; n < nmax; n++) {
			((CheckBox) m_tableAtts.getWidget(n, ATPos.SHOW.offset())).setEnabled(enabled);
			((ListBox) m_tableAtts.getWidget(n, ATPos.OP.offset())).setEnabled(enabled);
			((TextBox) m_tableAtts.getWidget(n, ATPos.VALUE.offset())).setEnabled(enabled);
		}
		m_type.setEnabled(enabled);
		m_useMediator.setEnabled(enabled);
		m_refresh.setEnabled(enabled);
		m_queryIntervalSecs.setEnabled(enabled);
		m_producerList[0].setEnabled(enabled);
		m_producerList[1].setEnabled(enabled);
		m_producerList[2].setEnabled(enabled);
	}

	private void stop() {
		enableButtons("");
		m_popTimer.cancel();
		setQueryControlsEnabled(true);
		m_run[0].setText("Run");
		m_run[1].setText("Run");
	}

	void addAttribute(String name, String type, int size) {
		int n = m_tableAtts.getRowCount();
		m_tableAtts.setText(n, ATPos.NAME.offset(), name);
		m_tableAtts.setText(n, ATPos.TYPE.offset(), type);
		CheckBox cb = new CheckBox();
		if (!name.startsWith("Rgma")) {
			cb.setValue(true);
		}
		m_tableAtts.setWidget(n, ATPos.SHOW.offset(), cb);
		ListBox lb = new ListBox();
		lb.setVisibleItemCount(1);
		lb.addItem("");
		lb.addItem(">");
		lb.addItem("<");
		lb.addItem("=");
		lb.addItem(">=");
		lb.addItem("<=");
		lb.addItem("<>");
		if (type.equals("VARCHAR") || type.equals("CHAR")) {
			lb.addItem("LIKE");
		}
		lb.setSelectedIndex(0);
		m_tableAtts.setWidget(n, ATPos.OP.offset(), lb);
		TextBox tb = new TextBox();
		if (size != 0) {
			tb.setWidth(Math.min(size, 30) + "em");
		}
		m_tableAtts.setWidget(n, ATPos.VALUE.offset(), tb);
	}

	void setTableName(String vdbName, String tableName, String viewFor) {
		stop();
		m_displayTimer.cancel();
		m_tableLabel.setText(vdbName + '.' + tableName);
		m_vdbName = vdbName;
		m_tableName = tableName;
		int nmax = m_tableAtts.getRowCount();
		for (int n = 1; n < nmax; n++) {
			m_tableAtts.removeRow(1);
		}
		getAllProducersForTable();
	}

}
