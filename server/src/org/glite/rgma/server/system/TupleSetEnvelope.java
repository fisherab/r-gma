package org.glite.rgma.server.system;

public class TupleSetEnvelope {

	private TupleSet m_tupleSet;

	private ResourceEndpoint m_source;

	private ResourceEndpoint m_target;

	private String m_query;

	public TupleSetEnvelope(TupleSet tupleSet, ResourceEndpoint source, ResourceEndpoint target, String query) {
		m_tupleSet = tupleSet;
		m_source = source;
		m_target = target;
		m_query = query;
	}

	public TupleSet getTupleSet() {
		return m_tupleSet;
	}

	public ResourceEndpoint getSource() {
		return m_source;
	}

	public ResourceEndpoint getTarget() {
		return m_target;
	}

	public String getQuery() {
		return m_query;
	}

}
