package org.glite.rgma.browser.client;

import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.ScrollPanel;

public class Log extends ScrollPanel {

	public static Log s_log;

	public static Log getInstance() {
		if (s_log == null) {
			s_log = new Log();
		}
		return s_log;
	}

	private FlexTable m_table;
	private boolean m_enabled = true;

	public Log() {
		setHeight("16em");
		m_table = new FlexTable();
		add(m_table);
	}

	public void add(String line) {
		if (m_enabled) {
			m_table.setText(m_table.getRowCount(), 0, line);
			scrollToBottom();
		}
	}

	public void disable() {
		m_enabled = false;
		setVisible(false);
	}

	public void enable() {
		m_enabled = true;
		setVisible(true);
	}

}
