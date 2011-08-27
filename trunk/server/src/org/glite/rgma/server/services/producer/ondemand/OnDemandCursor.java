package org.glite.rgma.server.services.producer.ondemand;

import org.glite.rgma.server.services.producer.store.TupleCursor;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.TupleSetWithLastTUID;
import org.glite.rgma.server.system.UserSystemContext;

public class OnDemandCursor implements TupleCursor {

	private OnDemandProducerResource m_onDemandProducerResource;
	private ResourceEndpoint m_consumerEp;
	private String m_vdbTableName;

	public OnDemandCursor(SelectStatement select, OnDemandProducerResource onDemandProducerResource, UserSystemContext context, ResourceEndpoint consumerEp, TimeInterval timeout, String vdbTableName) throws RGMAPermanentException {
		m_onDemandProducerResource = onDemandProducerResource;
		m_consumerEp = consumerEp;
		m_vdbTableName = vdbTableName;
		m_onDemandProducerResource.execute(select, context, m_vdbTableName, m_consumerEp, timeout);
	}

	public void close() throws RGMAPermanentException {
		m_onDemandProducerResource.closeQuery(m_vdbTableName, m_consumerEp);
	}

	/* A lastTUID of 0 is added - this is to comply with the interface offered by the TupleCursor */
	public TupleSetWithLastTUID pop(int maxCount) throws RGMAPermanentException {
		return new TupleSetWithLastTUID(m_onDemandProducerResource.pop(m_vdbTableName, m_consumerEp),0);
	}
}
