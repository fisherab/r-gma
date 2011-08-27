/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.mediator;

import java.util.ArrayList;
import java.util.List;

/**
 * A consumer's query plan. A query plan describes the producers a Consumer must contact to answer it's query.
 */
public class Plan {

	private final List<PlanEntry> m_producers;
	private String m_warning;

	public Plan(List<PlanEntry> producers, String warning) {
		m_producers = new ArrayList<PlanEntry>(producers);
		m_warning = warning.trim();
	}

	public List<PlanEntry> getPlanEntries() {
		return m_producers;
	}

	public String getWarning() {
		return m_warning;
	}

	public void setWarning(String warning) {
		m_warning = warning;
	}

	public boolean hasWarning() {
		return m_warning.length() > 0;
	}

	@Override
	public synchronized String toString() {
		StringBuffer s = new StringBuffer();
		for (PlanEntry pe : m_producers) {
			s.append(" [" + pe + "]");
		}
		if (m_warning.length() != 0) {
			s.append(" - ").append(m_warning);
		}
		return s.toString();
	}
}
