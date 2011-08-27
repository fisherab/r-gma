/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

import java.util.List;
import java.util.ArrayList;

/**
 * Authorization rules for a table.
 */
public class TableAuthorization {
    /**
     * List containing current table authorization rules.
     */
    private final List<String> m_rules;

    public TableAuthorization() {
        m_rules = new ArrayList<String>();
    }

    /**
     * Gets the number of rules in this TableAuthorization.
     *
     * @return The number of rules.
     */
    public int getNumRules() {
        return m_rules.size();
    }

    /**
     * Gets the specified rule.
     *
     * @param ruleNum Number of the rule to retrieve.
     *
     * @return Specified rule.
     */
    public String getRule(int ruleNum) {
        return m_rules.get(ruleNum);
    }

    /**
     * Returns the List of rules.
     *
     * @return The List of rules.
     */
    public List<String> getRules() {
        return m_rules;
    }

    /**
     * Adds a rule to this TableAuthorization.  A rule consists of a view on a
     * table (a parameterized SELECT statement) and a set of allowed
     * credentials specifying which users can access the view, separated by a
     * colon.
     *
     * @param rule Rule
     */
    public void addRule(String rule) {
        m_rules.add(rule);
    }
}
