package net.spy.nio.ops;

import java.util.concurrent.BlockingQueue;


/**
 * Factory used for creating operation queues.
 */
public interface OperationQueueFactory {

	/**
	 * Create an instance of a queue.
	 */
	BlockingQueue<Operation> create();

}
