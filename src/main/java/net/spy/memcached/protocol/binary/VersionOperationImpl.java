package net.spy.memcached.protocol.binary;

import net.spy.nio.ops.OperationCallback;
import net.spy.nio.ops.OperationStatus;
import net.spy.memcached.ops.VersionOperation;

class VersionOperationImpl extends OperationImpl implements VersionOperation {

	private static final int CMD = 11;

	public VersionOperationImpl(OperationCallback cb) {
		super(CMD, generateOpaque(), cb);
	}

	@Override
	public void initialize() {
		prepareBuffer("", 0, EMPTY_BYTES);
	}

	@Override
	protected void decodePayload(byte[] pl) {
		getCallback().receivedStatus(new OperationStatus(true, new String(pl)));
	}

}
