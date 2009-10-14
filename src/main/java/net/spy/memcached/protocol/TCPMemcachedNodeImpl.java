package net.spy.memcached.protocol;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;

import net.spy.memcached.MemcachedNode;
import net.spy.nio.ops.Operation;
import net.spy.nio.protocol.TCPServerNodeImpl;

/**
 * Represents a node with the memcached cluster, along with buffering and
 * operation queues.
 */
public abstract class TCPMemcachedNodeImpl extends TCPServerNodeImpl implements MemcachedNode {

	public TCPMemcachedNodeImpl(SocketAddress sa, SocketChannel c, int bufSize, BlockingQueue<Operation> rq, BlockingQueue<Operation> wq, BlockingQueue<Operation> iq, boolean shouldOptimize) {
		super(sa, c, bufSize, rq, wq, iq, shouldOptimize);
	}

}