package net.spy.memcached.ops;

import net.spy.nio.ops.MultiOperationCallback;
import net.spy.nio.ops.OperationCallback;
import net.spy.nio.ops.OperationStatus;

/**
 * MultiOperationCallback for get operations.
 */
public class MultiGetOperationCallback extends MultiOperationCallback
	implements GetOperation.Callback {

	public MultiGetOperationCallback(OperationCallback original, int todo) {
		super(original, todo);
	}

	public void gotData(String key, int flags, byte[] data) {
		((GetOperation.Callback)originalCallback).gotData(key, flags, data);
	}

	@Override
	public void complete() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void receivedStatus(OperationStatus status) {
		// TODO Auto-generated method stub
		
	}

}
