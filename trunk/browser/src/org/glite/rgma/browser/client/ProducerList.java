package org.glite.rgma.browser.client;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

public class ProducerList extends VerticalPanel {

	private boolean m_hrpneeded;
	private FlexTable m_table;
	private List<String> m_urls = new ArrayList<String>();

	public ProducerList(String labelString, boolean hrpneeded) {
		Label l = new Label(labelString);
		l.addStyleName(Style.ROWHEADER);
		add(l);
		m_hrpneeded = hrpneeded;
		m_table = new FlexTable();
		m_table.addStyleName(Style.DATATABLE);
		add(m_table);
		m_table.setText(0, 0, "Hostname");
		m_table.setText(0, 1, "ID");
		if (hrpneeded) {
			m_table.setText(0, 2, "HRP (secs)");
		}
		m_table.setText(0, 3, "Selected");
		m_table.getRowFormatter().addStyleName(0, Style.COLUMNHEADER);
	}

	public void appendListString(StringBuilder sb) {
		int nrow = m_table.getRowCount();
		for (int n = 1; n < nrow; n++) {
			Widget w = m_table.getWidget(n, 3);
			if (((CheckBox) w).getValue()) {
				String endpointString = m_table.getText(n, 1) + " " + m_urls.get(n - 1);
				sb.append("&producerConnections=" + endpointString);
			}
		}
	}

	public void clear() {
		while (m_table.getRowCount() > 1) {
			m_table.removeRow(1);
		}
		m_urls.clear();
	}

	public void add(String url, int id, int hrpSec) {
		int n = m_table.getRowCount();
		String host = url.substring(8);
		host = host.substring(0, host.indexOf(':'));
		m_table.setText(n, 0, host);
		m_table.setText(n, 1, Integer.toString(id));
		if (m_hrpneeded) {
			m_table.setText(n, 2, Integer.toString(hrpSec));
		}
		CheckBox cb = new CheckBox();
		m_table.setWidget(n, 3, cb);
		m_urls.add(url);
	}

	public void setEnabled(boolean enabled) {
		int nmax = m_table.getRowCount();
		for (int n = 1; n < nmax; n++) {
			((CheckBox) m_table.getWidget(n, 3)).setEnabled(enabled);
		}
	}
}
