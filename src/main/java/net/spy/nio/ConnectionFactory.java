package net.spy.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import net.spy.nio.ops.Operation;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Factory for creating instances of MemcachedConnection.
 * This is used to provide more fine-grained configuration of connections.
 */
public interface ConnectionFactory {

	/**
	 * Create a MemcachedConnection for the given SocketAddresses.
	 *
	 * @param addrs the addresses of the memcached servers
	 * @return a new MemcachedConnection connected to those addresses
	 * @throws IOException for problems initializing the memcached connections
	 */
	ServerConnection createConnection(List<InetSocketAddress> addrs)
		throws IOException;

	/**
	 * Create a new memcached node.
	 */
	ServerNode createServerNode(SocketAddress sa,
			SocketChannel c, int bufSize);

	/**
	 * Create a BlockingQueue for operations for a connection.
	 */
	BlockingQueue<Operation> createOperationQueue();

	/**
	 * Create a BlockingQueue for the operations currently expecting to read
	 * responses from memcached.
	 */
	BlockingQueue<Operation> createReadOperationQueue();

	/**
	 * Create a BlockingQueue for the operations currently expecting to write
	 * requests to memcached.
	 */
	BlockingQueue<Operation> createWriteOperationQueue();

	/**
	 * Create a NodeLocator instance for the given list of nodes.
	 */
	ServerNodeLocator createLocator(List<ServerNode> nodes);

	/**
	 * Get the operation factory for connections built by this connection
	 * factory.
	 */
	OperationFactory getOperationFactory();

	/**
	 * Get the operation timeout used by this connection.
	 */
	long getOperationTimeout();

	/**
	 * If true, the IO thread should be a daemon thread.
	 */
	boolean isDaemon();

	/**
	 * If true, the nagle algorithm will be used on connected sockets.
	 *
	 * <p>
	 * See {@link java.net.Socket#setTcpNoDelay(boolean)} for more information.
	 * </p>
	 */
	boolean useNagleAlgorithm();

	/**
	 * Observers that should be established at the time of connection
	 * instantiation.
	 *
	 * These observers will see the first connection established.
	 */
	Collection<ConnectionObserver> getInitialObservers();

	/**
	 * Get the default failure mode for the underlying connection.
	 */
	FailureMode getFailureMode();

	/**
	 * Get the default transcoder to be used in connections created by this
	 * factory.
	 */
	Transcoder<Object> getDefaultTranscoder();

	/**
	 * If true, low-level optimization is in effect.
	 */
	boolean shouldOptimize();

	/*
	 * Get the read buffer size set at construct time.
	 */
	int getReadBufSize();

	/**
	 * Get the hash algorithm to be used.
	 */
	public HashAlgorithm getHashAlg();
}
