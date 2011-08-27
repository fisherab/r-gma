/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.mediator;

import org.glite.rgma.server.system.RGMAPermanentException;

import org.glite.rgma.server.services.sql.SelectStatement;

/**
 * An instruction to modify a query plan.
 *
 * Requests the addition or removal of a producer from
 * a Consumer's query plan.
 */
public class PlanInstruction
{
    public enum Type {
        /** Instruction to add a producer to a plan */
        ADD,
        /** Instruction to remove a producer from a plan */
        REMOVE
    }

    private final Type m_type;
    private final ProducerDetails m_producer;
    private final SelectStatement m_select;
    private final String m_warning;

    /**
     * Constructor
     *
     * @param type One of <code>PlanInstruction.ADD</code> or
     *             <code>PlanInstruction.REMOVE</code>
     * @param producer Plan entry the instruction refers to.
     * @param warning Warning to be added to the plan.
     */
    public PlanInstruction(ProducerDetails producer, Type type, SelectStatement select, String warning)
        throws RGMAPermanentException
    {
        if ((select == null) && (type == Type.ADD)) {
            throw new RGMAPermanentException("Cannot add a producer without specifying a query");
        }
        m_type = type;
        m_producer = producer;
        m_warning = warning;
        m_select = select;
    }

    /**
     * Constructor
     *
     * @param type One of <code>PlanInstruction.ADD</code> or
     *             <code>PlanInstruction.REMOVE</code>
     * @param producer Plan entry the instruction refers to.
     */
    public PlanInstruction(ProducerDetails producer, Type type, String warning)
        throws RGMAPermanentException
    {
        this(producer, type, null, warning);
    }

    /**
     * Constructor
     *
     * @param type One of <code>PlanInstruction.ADD</code> or
     *             <code>PlanInstruction.REMOVE</code>
     * @param producer Plan entry the instruction refers to.
     */
    public PlanInstruction(ProducerDetails producer, Type type)
        throws RGMAPermanentException
    {
        this(producer, type, null, "");
    }

    /**
     * Constructor
     *
     * @param type One of <code>PlanInstruction.ADD</code> or
     *             <code>PlanInstruction.REMOVE</code>
     * @param producer Plan entry the instruction refers to.
     */
    public PlanInstruction(ProducerDetails producer, Type type, SelectStatement select)
        throws RGMAPermanentException
    {
        this(producer, type, select, "");
    }

    /**
     * Get the type of instruction.
     *
     * @return One of <code>PlanInstruction.ADD</code> or
     *         <code>PlanInstruction.REMOVE</code>
     */
    public Type getType()
    {
        return m_type;
    }

    /**
     * Get the plan entry the instruction refers to.
     */
    public ProducerDetails getProducer()
    {
        return m_producer;
    }

    /**
     * Get the plan entry the instruction refers to.
     */
    public SelectStatement getSelect()
    {
        return m_select;
    }

    public String getWarning()
    {
        return m_warning;
    }

    public boolean hasWarning()
    {
        return m_warning.length() > 0;
    }
}
