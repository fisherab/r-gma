package org.glite.rgma.browser.client;

import com.google.gwt.event.dom.client.KeyDownEvent;
import com.google.gwt.event.dom.client.KeyDownHandler;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextBox;

public class IntegerInput extends Composite {

	@SuppressWarnings("serial")
	class IntegerInputException extends Exception {

		IntegerInputException() {
			super("Invalid integer input in '" + m_label.getText() + '"');
		}

	}

	private TextBox m_textBox = new TextBox();
	private Label m_label = new Label();
	private Label m_units = new Label();

	public IntegerInput(String label, String units) {
		HorizontalPanel panel = new HorizontalPanel();
		panel.setVerticalAlignment(HasVerticalAlignment.ALIGN_MIDDLE);
		m_label.setText(label);
		m_label.addStyleName("BigLeft");
		panel.add(m_label);
		m_textBox.setWidth("5em");
		m_textBox.addStyleName("SmallLeft");
		panel.add(m_textBox);
		m_units.setText(units);
		m_units.addStyleName("SmallLeft");
		panel.add(m_units);
		initWidget(panel);
		init();
	}

	public IntegerInput(String label) {
		HorizontalPanel panel = new HorizontalPanel();
		m_label.setText(label);
		m_label.addStyleName("BigLeft");
		panel.add(m_label);
		m_textBox.setWidth("5em");
		m_textBox.addStyleName("SmallLeft");
		panel.add(m_textBox);
		initWidget(panel);
		init();
	}
	
	void init() {
		m_textBox.addKeyDownHandler(new KeyDownHandler() {
			
			@Override
			public void onKeyDown(KeyDownEvent event) {
				m_textBox.removeStyleName("Error");
				
			}
		});
	}


	public void setValue(int value) {
		m_textBox.setText(Integer.toString(value));
	}

	public boolean isBlank() {
		return m_textBox.getText().trim().length() == 0;
	}

	public int getValue() throws IntegerInputException {
		if (isBlank()){
			throw new IntegerInputException();
		}
		try {
			return Integer.parseInt(m_textBox.getText().trim());
		} catch (NumberFormatException e) {
			m_textBox.addStyleName("Error");
			throw new IntegerInputException();
		}
	}

	public void setEnabled(boolean enabled) {
		m_textBox.setEnabled(enabled);

	}
}
