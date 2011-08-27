package org.glite.rgma.server.services.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.apache.log4j.Logger;
import org.glite.rgma.server.system.RGMAPermanentException;

public abstract class StreamingEndpoint extends Thread {
	
	protected Logger LOG;

	protected void readData(SelectionKey key) throws RGMAPermanentException {
		SocketChannel channel = (SocketChannel) key.channel();
		if (!channel.isOpen()) {
			// Channel could be closed, but not yet deregistered.
			LOG.warn("readData called on closed channel");
			return;
		}
		StreamingSSLEngine sseng = (StreamingSSLEngine) key.attachment();
		ByteBuffer encryptedReadBuffer = sseng.getEncryptedReadBuffer();
		int bytesRead;
		try {
			bytesRead = channel.read(encryptedReadBuffer);
		} catch (IOException e) {
			clearKey(key);
			LOG.warn("Error reading from channel so closed it for " + sseng + " " + e.getMessage());
			return;
		}
		if (bytesRead > 0) {
			encryptedReadBuffer.flip();
			while (encryptedReadBuffer.remaining() != 0) {
				sseng.pushBytes();
				HandshakeStatus hs = sseng.getHandshakeStatus();
				if (hs == HandshakeStatus.NEED_WRAP || hs == HandshakeStatus.FINISHED) {
					key.interestOps(SelectionKey.OP_WRITE + SelectionKey.OP_READ);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Key with " + sseng + " NEED_WRAP or FINISHED so set to READ WRITE interest");
					}
					break;
				}
				if (sseng.getEngineOpStatus() != SSLEngineResult.Status.OK) {
					break;
				}
			}
			encryptedReadBuffer.compact();
		} else if (bytesRead < 0) {
			clearKey(key);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Channel for " + sseng + " closed as EOF encountered");
			}
		} else {
			clearKey(key);
			LOG.error("No handshake bytes read from read buffer " + encryptedReadBuffer + " for " + sseng + ". Channel closed to allow recovery");
		}
	}

	protected abstract void clearKey(SelectionKey key) throws RGMAPermanentException;

	
}
