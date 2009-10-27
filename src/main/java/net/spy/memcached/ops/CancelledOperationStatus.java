package net.spy.memcached.ops;

import net.spy.nio.ops.OperationStatus;

/**
 * Operation status indicating an operation was cancelled.
 */
public class CancelledOperationStatus extends OperationStatus {

	public CancelledOperationStatus() {
		super(false, "cancelled");
	}

}
