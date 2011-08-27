package org.glite.rgma.server.system;

public class TupleSetWithLastTUID {

	private TupleSet m_tupleSet;
	private int m_lastTUID;

	public TupleSetWithLastTUID(TupleSet tupleSet, int lastTUID) {
		m_tupleSet = tupleSet;
		m_lastTUID = lastTUID;
	}

	public TupleSet getTupleSet() {
		return m_tupleSet;
	}

	public int getLastTUID() {
		return m_lastTUID;
	}
}
