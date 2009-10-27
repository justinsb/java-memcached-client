package net.spy.nio;

import java.util.concurrent.CountDownLatch;

import net.spy.nio.ops.Operation;

/**
 * Factory for creating Operations to be broadcast.
 */
public interface BroadcastOpFactory {

	/**
	 * Construct a new operation for delivery to the given node.
	 * Each operation should count the given latch down upon completion.
	 */
	Operation newOp(ServerNode n, CountDownLatch latch);
}
