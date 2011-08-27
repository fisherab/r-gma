package org.glite.rgma.browser.client;

import org.glite.rgma.browser.client.IntegerInput.IntegerInputException;

import com.google.gwt.http.client.Response;
import com.google.gwt.user.client.ui.Label;

public class Message extends Label {

	void display(String message) {
		removeStyleName("Error");
		setText(message);
	}

	void display(Response response) {
		displayError("Server connection problem: " + response.getText().trim() + " - " + response.getStatusText());
	}

	private void displayError(String message) {
		addStyleName("Error");
		setText(message);
	}

	void display(Throwable e) {
		displayError(e.getClass().getName() + " " + e.getMessage());
	}

	void display(RGMAException e) {
		displayError(e.getMessage());
	}
	
	void display(IntegerInputException e) {
		displayError(e.getMessage());
	}

	void clear() {
		removeStyleName("Error");
		setText("");
	}
}
