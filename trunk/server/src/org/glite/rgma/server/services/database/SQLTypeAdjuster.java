package org.glite.rgma.server.services.database;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import org.glite.rgma.server.system.RGMAPermanentException;

public class SQLTypeAdjuster {

	static SimpleDateFormat dateFormat;

	static Pattern s_afterDot = Pattern.compile("\\d*");

	static SimpleDateFormat timeFormat;
	static SimpleDateFormat timestampFormat;

	static {
		timeFormat = new SimpleDateFormat("HH:mm:ss");
		timeFormat.setLenient(false);
		timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		timestampFormat.setLenient(false);
		dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		dateFormat.setLenient(false);
	}

	public static String checkDate(String input) throws RGMAPermanentException {
		input = input.trim();
		try {
			synchronized (dateFormat) {
				if (input.length() != 10) {
					throw new RGMAPermanentException("Invalid data length");
				}
				dateFormat.parse(input);
			}
		} catch (ParseException e) {
			throw new RGMAPermanentException("Invalid date");
		}
		return input;
	}

	/*
	 * HSQLDB limits us to no decimal point for the Time while MySQL would allow up to 6 figures after the decimal
	 * point.
	 */
	public static String checkTime(String input) throws RGMAPermanentException {
		input = input.trim();
		String timeString;
		try {
			int i = input.indexOf('.');
			if (i >= 0) {
				String rest = input.substring(i + 1);
				timeString = input.substring(0, i);
				if (!s_afterDot.matcher(rest).matches()) {
					throw new RGMAPermanentException("");
				}
			} else {
				timeString = input;
			}
			synchronized (timeFormat) {
				if (timeString.length() != 8) {
					throw new RGMAPermanentException("");
				}
				timeFormat.parse(timeString);
			}
		} catch (ParseException e) {
			throw new RGMAPermanentException("");
		}
		return timeString;
	}

	/*
	 * MySQL limits us to six figures after the decimal point for the Timestamp while HSQLDB would allow 9.
	 */
	public static String checkTimestamp(String input) throws RGMAPermanentException {
		input = input.trim();
		String timestampString;
		String rest = "";
		try {
			int i = input.indexOf('.');
			if (i >= 0) {
				rest = input.substring(i + 1);
				timestampString = input.substring(0, i);
				if (!s_afterDot.matcher(rest).matches()) {
					throw new RGMAPermanentException("Timestamp does not match after dot: " + input);
				}
			} else {
				timestampString = input;
			}
			synchronized (timestampFormat) {
				if (timestampString.length() != 19) {
					throw new RGMAPermanentException("Timestamp error: timestamp \"" + input + "\" length " + timestampString.length() + " is not equal to 19");
				}
				timestampFormat.parse(timestampString);
			}
		} catch (ParseException e) {
			throw new RGMAPermanentException("Timestamp parse exception occured " + e.getMessage());
		}
		if (rest.length() == 0) {
			return timestampString;
		} else {
			return input.substring(0, Math.min(26, input.length()));
		}
	}

	public static String checkInteger(String input) throws RGMAPermanentException {
		input = input.trim();
		try {
			Integer.parseInt(input);
			return input;
		} catch (NumberFormatException e1) {
			if (input.indexOf('.') < 0 && input.indexOf('E') < 0) {
				throw new RGMAPermanentException("Integer checker error: no . or index of E found " + input);
			}
			try {
				float value = Float.parseFloat(input);
				if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
					throw new RGMAPermanentException("Integer checker error: out of range " + input);
				}
				return String.valueOf((int) value);
			} catch (NumberFormatException e2) {
				throw new RGMAPermanentException("Integer checker error: number format exception: " + e2.getMessage());
			}
		}

	}
}
