/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */
package org.glite.rgma.server.services.tasks;


/**
 * Data about a TaskInvocator and the current task that it is running.
 *
 */
class TaskInvocatorData {
    private TaskInvocator m_invocatorRef;
    private boolean m_goodOnlyInvocator;
    private Task m_currentTask;
    private long m_taskEndTimeMillis;

    /**
     * Creates a new TaskInvocatorData object.
     *
     * @param invocatorRef
     *            A reference to the TaskInvocator object
     * @param goodOnly
     *            Indicates whether the TaskInvocator only accepts good tasks
     * @param currentTask
     *            The task being processed by the TaskInvocator
     * @param taskEndTimeMillis
     *            The time by which the task should have finished
     */
    TaskInvocatorData(TaskInvocator invocatorRef, boolean goodOnlyInvocator, Task currentTask, long taskEndTimeMillis) {
        m_invocatorRef = invocatorRef;
        m_goodOnlyInvocator = goodOnlyInvocator;
        m_currentTask = currentTask;
        m_taskEndTimeMillis = taskEndTimeMillis;
    }

    /**
     * Returns a reference to the TaskInvocator
     *
     * @return TaskInvocator
     */
    TaskInvocator getInvocatorRef() {
        return m_invocatorRef;
    }

    /**
     * Returns the task being processed by the TaskInvocator
     *
     * @return Task
     */
    Task getTask() {
        return m_currentTask;
    }

    /**
     * Returns the time by which the task should have finished
     *
     * @return task end time in millis
     */
    long getTaskEndTimeMillis() {
        return m_taskEndTimeMillis;
    }

    /**
     * Returns flag that indicates whether the TaskInvocator only accepts good
     * tasks
     *
     * @return goodOnly flag
     */
    boolean isGoodOnlyInvocator() {
        return m_goodOnlyInvocator;
    }
}
