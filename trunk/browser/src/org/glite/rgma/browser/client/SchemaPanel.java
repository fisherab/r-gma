package org.glite.rgma.browser.client;

import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.VerticalPanel;

public class SchemaPanel extends VerticalPanel {

	private final Label m_tableLabel;
	private FlexTable m_rows;

	SchemaPanel() {
		setWidth("100%");
		m_tableLabel = new Label();
		add(m_tableLabel);
		m_rows = new FlexTable();
		add(m_rows);
	}

	public void addAttribute(String name, String type, Boolean primary, Boolean notnull) {
		final int n = m_rows.getRowCount();
		m_rows.setText(n, 0, name);
		m_rows.setText(n, 1, type + (primary ? " PRIMARY KEY" : "") + (notnull ? " NOT NULL" : ""));
	}

	public void setTableName(String vdbName, String tableName, String viewFor) {
		String tableLabel = viewFor == null ? "Table " + vdbName + '.' + tableName : "View " + vdbName + '.' + tableName
				+ " on table " + vdbName + '.' + viewFor;

		m_tableLabel.setText(tableLabel);
		while (m_rows.getRowCount() > 0) {
			m_rows.removeRow(0);
		}
	}
}
