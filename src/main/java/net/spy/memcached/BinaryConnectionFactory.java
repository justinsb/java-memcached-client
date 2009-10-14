package net.spy.memcached;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.nio.HashAlgorithm;

/**
 * Default connection factory for binary wire protocol connections.
 */
public class BinaryConnectionFactory extends DefaultMemcachedConnectionFactory {

	/**
	 * Create a DefaultConnectionFactory with the default parameters.
	 */
	public BinaryConnectionFactory() {
		super();
	}

	/**
	 * Create a BinaryConnectionFactory with the given maximum operation
	 * queue length, and the given read buffer size.
	 */
	public BinaryConnectionFactory(int len, int bufSize) {
		super(len, bufSize);
	}

	/**
	 * Construct a BinaryConnectionFactory with the given parameters.
	 *
	 * @param hashAlgorithm the algorithm to use for hashing
	 * @param bufSize the buffer size
	 * @param qLen the queue length.
	 */
	public BinaryConnectionFactory(int len, int bufSize, HashAlgorithm hash) {
		super(len, bufSize, hash);
	}

	@Override
	public MemcachedNode createServerNode(SocketAddress sa,
			SocketChannel c, int bufSize) {
		return new BinaryMemcachedNodeImpl(sa, c, bufSize,
			createReadOperationQueue(),
			createWriteOperationQueue(),
			createOperationQueue(),
			shouldOptimize());
	}

	@Override
	public OperationFactory getOperationFactory() {
		return new BinaryOperationFactory();
	}

}
