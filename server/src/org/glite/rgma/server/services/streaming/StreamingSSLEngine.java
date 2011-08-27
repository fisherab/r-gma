package org.glite.rgma.server.services.streaming;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;

import org.apache.log4j.Logger;
import org.glite.rgma.server.system.RGMAPermanentException;

public abstract class StreamingSSLEngine {

	protected Logger LOG;

	protected SSLEngine m_sslEngine;

	protected List<ByteBuffer> m_writeBuffers = new LinkedList<ByteBuffer>();

	protected ByteBuffer m_encryptedWriteBuffer;

	protected ByteBuffer m_readBuffer;

	protected ByteBuffer m_encryptedReadBuffer;

	protected HandshakeStatus m_handshakeStatus;

	protected Status m_engineOpStatus;

	protected int m_bytesSinceHandshake;

	public abstract void pushBytes() throws RGMAPermanentException;

	private void processTasks(SSLEngineResult result) {
		m_handshakeStatus = result.getHandshakeStatus();
		m_engineOpStatus = result.getStatus();
		if (m_handshakeStatus == HandshakeStatus.NEED_TASK) {
			Runnable runnable;
			while ((runnable = m_sslEngine.getDelegatedTask()) != null) {
				runnable.run();
			}
			m_handshakeStatus = m_sslEngine.getHandshakeStatus();
			LOG.debug(this + " ran delegated tasks - new HandshakeStatus " + m_handshakeStatus);
		}
	}

	protected ByteBuffer getEncryptedReadBuffer() {
		return m_encryptedReadBuffer;
	}

	protected Status getEngineOpStatus() {
		return m_engineOpStatus;
	}

	protected HandshakeStatus getHandshakeStatus() {
		return m_handshakeStatus;
	}

	protected void unwrap() throws RGMAPermanentException {
		int erb1 = 0, erb2 = 0, rb1 = 0, rb2 = 0;
		if (LOG.isDebugEnabled()) {
			erb1 = m_encryptedReadBuffer.remaining();
			erb2 = m_encryptedReadBuffer.capacity() - m_encryptedReadBuffer.limit();
			rb1 = m_readBuffer.position();
			rb2 = m_readBuffer.remaining();
		}
		SSLEngineResult res;
		try {
			res = m_sslEngine.unwrap(m_encryptedReadBuffer, m_readBuffer);
			if (LOG.isDebugEnabled()) {
				LOG.debug("ERB " + erb1 + "/" + erb2 + " RB " + rb1 + "/" + rb2 + " UNWRAP ERB " + m_encryptedReadBuffer.remaining() + "/"
						+ (m_encryptedReadBuffer.capacity() - m_encryptedReadBuffer.limit()) + " RB " + m_readBuffer.position() + "/"
						+ m_readBuffer.remaining() + " " + res.getStatus() + " " + res.getHandshakeStatus());
			}
			processTasks(res);
		} catch (SSLException e) {
			throw new RGMAPermanentException(e);
		}
	}

	protected ByteBuffer wrap() throws RGMAPermanentException {
		for (ByteBuffer b : m_writeBuffers) {
			b.flip();
		}
		List<Integer> wb1s = new ArrayList<Integer>(0);
		List<Integer> wb2s = new ArrayList<Integer>(0);
		int ewb1 = 0, ewb2 = 0, toWrite = 0;
		for (ByteBuffer b : m_writeBuffers) {
			wb1s.add(b.remaining());
			wb2s.add(b.capacity() - b.limit());
			toWrite += b.remaining();
		}
		ewb1 = m_encryptedWriteBuffer.position();
		ewb2 = m_encryptedWriteBuffer.remaining();
		try {
			SSLEngineResult result = m_sslEngine.wrap(m_writeBuffers.toArray(new ByteBuffer[0]), m_encryptedWriteBuffer);
			Iterator<ByteBuffer> iter = m_writeBuffers.iterator();
			while (iter.hasNext()) {
				ByteBuffer b = iter.next();
				if (b.remaining() == 0) {
					iter.remove();
				} else {
					toWrite = toWrite -= b.remaining();
					b.compact();
				}
			}
			if (LOG.isDebugEnabled()) {
				StringBuilder sb = new StringBuilder("WB ");
				for (int i = 0; i < wb1s.size(); i++) {
					sb.append(wb1s.get(i) + "/" + wb2s.get(i) + " ");
				}
				sb.append(" EWB " + ewb1 + "/" + ewb2 + " WRAP WB ");
				for (ByteBuffer b : m_writeBuffers) {
					sb.append(b.position() + "/" + b.remaining() + " ");
				}
				sb.append("EWB " + m_encryptedWriteBuffer.position() + "/" + m_encryptedWriteBuffer.remaining() + " " + result.getStatus() + " "
						+ result.getHandshakeStatus());
				LOG.debug(sb);
			}
			processTasks(result);
		} catch (SSLException e) {
			throw new RGMAPermanentException(e);
		}
		m_bytesSinceHandshake += toWrite;
		return m_encryptedWriteBuffer;
	}

}
