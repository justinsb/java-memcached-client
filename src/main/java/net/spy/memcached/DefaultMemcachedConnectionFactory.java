package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;

import net.spy.memcached.protocol.ascii.AsciiMemcachedNodeImpl;
import net.spy.memcached.protocol.ascii.AsciiOperationFactory;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.nio.DefaultConnectionFactory;
import net.spy.nio.HashAlgorithm;

public class DefaultMemcachedConnectionFactory extends DefaultConnectionFactory implements MemcachedConnectionFactory {
	
	
	public DefaultMemcachedConnectionFactory() {
		super();
	}

	public DefaultMemcachedConnectionFactory(int qLen, int bufSize, HashAlgorithm hash) {
		super(qLen, bufSize, hash);
	}

	public DefaultMemcachedConnectionFactory(int qLen, int bufSize) {
		super(qLen, bufSize);
	}

	public MemcachedNode createServerNode(SocketAddress sa,
			SocketChannel c, int bufSize) {

		OperationFactory of = getOperationFactory();
		if(of instanceof AsciiOperationFactory) {
			return new AsciiMemcachedNodeImpl(sa, c, bufSize,
				createReadOperationQueue(),
				createWriteOperationQueue(),
				createOperationQueue());
		} else if(of instanceof BinaryOperationFactory) {
			return new BinaryMemcachedNodeImpl(sa, c, bufSize,
					createReadOperationQueue(),
					createWriteOperationQueue(),
					createOperationQueue());
		} else {
			throw new IllegalStateException(
				"Unhandled operation factory type " + of);
		}
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.ConnectionFactory#createConnection(java.util.List)
	 */
	public MemcachedConnection createConnection(List<InetSocketAddress> addrs)
		throws IOException {
		return new MemcachedConnection(getReadBufSize(), this, addrs,
			getInitialObservers(), getFailureMode(), getOperationFactory());
	}


	/* (non-Javadoc)
	 * @see net.spy.memcached.ConnectionFactory#getOperationFactory()
	 */
	public OperationFactory getOperationFactory() {
		return new AsciiOperationFactory();
	}

}
