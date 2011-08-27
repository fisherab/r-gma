/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.system;

/**
 * Exception thrown when an error occurs in the RGMA application.
 */
@SuppressWarnings("serial")
public class RGMAException extends Exception {

	/** Number of successful batch operations. */
	private int m_numSuccessfulOps;
     
    protected RGMAException(String message, Throwable cause) {
        super(message, cause);
    }
    
    protected RGMAException(Throwable cause) {
        super(cause);
    }

    protected RGMAException(String message) {
        super(message);
    }
    
    public String getFlattenedMessage() {
    	if (getCause() == null) {
    		return getMessage();
    	} else {
    		StringBuilder msg = new StringBuilder();
    		msg.append(getMessage());
   
    		Throwable t = getCause();
    		while (t != null) {
    			msg.append( " caused by (");
    			msg.append(t);
    			msg.append(")");
    			t = t.getCause();
    		}
    		return msg.toString();
    	}
    }

    /**
     * Returns the number of successful operations (for batch commands).
     *
     * @return The number of successful operations (for batch commands).
     */
    public int getNumSuccessfulOps() {
        return m_numSuccessfulOps;
    }

    /**
     * Sets the number of successful operations (for batch commands).
     *
     * @param numSuccessfulOps Number of successful operations (for batch commands).
     */
    public void setNumSuccessfulOps(int numSuccessfulOps) {
        m_numSuccessfulOps = numSuccessfulOps;
    }
}
