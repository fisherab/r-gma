package org.glite.rgma;

/**
 * Types of query supported by a {@link PrimaryProducer} or {@link SecondaryProducer}.
 */
public enum SupportedQueries {

	/** Continuous queries only. */
	C,

	/** Continuous and History queries. */
	CH,

	/** Continuous and Latest queries. */
	CL,

	/** Continuous, History and Latest queries. */
	CHL;
	
	boolean isContinuous() {
		return (true);
	}
	boolean isHistory() {
		return (this == CH || this == CHL);
	}
	boolean isLatest() {
		return (this == CL || this == CHL);
	}
}
