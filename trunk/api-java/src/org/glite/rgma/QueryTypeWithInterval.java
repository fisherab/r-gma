package org.glite.rgma;

/**
 * Permitted query type for a consumer where an interval for the query should be specified.
 */
public enum QueryTypeWithInterval {
	/** Continuous Query. */
	C,

	/** History Query. */
	H,

	/** Latest Query. */
	L
}
