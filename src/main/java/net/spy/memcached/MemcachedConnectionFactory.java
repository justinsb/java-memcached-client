package net.spy.memcached;

import net.spy.memcached.transcoders.Transcoder;
import net.spy.nio.ConnectionFactory;

public interface MemcachedConnectionFactory extends ConnectionFactory {
	/**
	 * Get the operation factory for connections built by this connection
	 * factory.
	 */
	OperationFactory getOperationFactory();

	/**
	 * Get the default transcoder to be used in connections created by this
	 * factory.
	 */
	Transcoder<Object> getDefaultTranscoder();

}
