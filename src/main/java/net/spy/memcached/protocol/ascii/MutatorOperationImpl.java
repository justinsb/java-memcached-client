// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.nio.KeyUtil;
import net.spy.memcached.ops.MutatorOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.nio.ops.OperationCallback;
import net.spy.nio.ops.OperationState;
import net.spy.nio.ops.OperationStatus;

/**
 * Operation for mutating integers inside of memcached.
 */
final class MutatorOperationImpl extends OperationImpl
	implements MutatorOperation {

	public static final int OVERHEAD=32;

	private static final OperationStatus NOT_FOUND=
		new OperationStatus(false, "NOT_FOUND");

	private final Mutator mutator;
	private final String key;
	private final int amount;

	public MutatorOperationImpl(Mutator m, String k, int amt,
			OperationCallback c) {
		super(c);
		mutator=m;
		key=k;
		amount=amt;
	}

	@Override
	public void handleLine(String line) {
		getLogger().debug("Result:  %s", line);
		OperationStatus found=null;
		if(line.equals("NOT_FOUND")) {
			found=NOT_FOUND;
		} else {
			found=new OperationStatus(true, line);
		}
		getCallback().receivedStatus(found);
		transitionState(OperationState.COMPLETE);
	}

	@Override
	public void initialize() {
		int size=KeyUtil.getKeyBytes(key).length + OVERHEAD;
		ByteBuffer b=ByteBuffer.allocate(size);
		setArguments(b, mutator.name(), key, amount);
		b.flip();
		setBuffer(b);
	}

	@Override
	protected void wasCancelled() {
		// XXX:  Replace this comment with why the hell I did this.
		getCallback().receivedStatus(CANCELLED);
	}

	public Collection<String> getKeys() {
		return Collections.singleton(key);
	}

	public int getBy() {
		return amount;
	}

	public long getDefault() {
		return -1;
	}

	public int getExpiration() {
		return -1;
	}

	public Mutator getType() {
		return mutator;
	}

}
