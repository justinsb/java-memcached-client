package net.spy.nio.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.ops.CancelledOperationStatus;
import net.spy.nio.ServerNode;
import net.spy.nio.ops.OperationCallback;
import net.spy.nio.ops.OperationErrorType;
import net.spy.nio.ops.OperationException;
import net.spy.nio.ops.OperationState;
import net.spy.nio.ops.OperationStatus;

/**
 * Base class for protocol-specific operation implementations.
 */
public abstract class BaseOperationImpl extends SpyObject {

	/**
	 * Status object for cancelled operations.
	 */
	public static final OperationStatus CANCELLED =
		new CancelledOperationStatus();
	private OperationState state = OperationState.WRITING;
	private ByteBuffer buffer = null;
	private boolean cancelled = false;
	private OperationException exception = null;
	protected OperationCallback callback = null;
	private volatile ServerNode handlingNode = null;

	public BaseOperationImpl() {
		super();
	}

	/**
	 * Get the operation callback associated with this operation.
	 */
	public final OperationCallback getCallback() {
		return callback;
	}

	/**
	 * Set the callback for this instance.
	 */
	protected void setCallback(OperationCallback to) {
		callback=to;
	}

	public final boolean isCancelled() {
		return cancelled;
	}

	public final boolean hasErrored() {
		return exception != null;
	}

	public final OperationException getException() {
		return exception;
	}

	public final void cancel() {
		cancelled=true;
		wasCancelled();
		callback.complete();
	}

	/**
	 * This is called on each subclass whenever an operation was cancelled.
	 */
	protected void wasCancelled() {
		getLogger().debug("was cancelled.");
	}

	public final OperationState getState() {
		return state;
	}

	public final ByteBuffer getBuffer() {
		if (buffer == null) {
			ByteBuffer bb = buildBuffer();
			bb.mark();
			buffer = bb;
		}
		return buffer;
	}
	
	public final boolean hasBuiltBuffer() {
		return buffer != null;
	}
	
	/**
	 * Build the message for this operation
	 */
	protected abstract ByteBuffer buildBuffer();

	/**
	 * Transition the state of this operation to the given state.
	 */
	protected final void transitionState(OperationState newState) {
		getLogger().debug("Transitioned state from %s to %s", state, newState);
		state=newState;
		// Discard our buffer when we no longer need it.
		if(state != OperationState.WRITING) {
			buffer=null;
		}
		if(state == OperationState.COMPLETE) {
			callback.complete();
		}
	}

	public final void writeComplete() {
		transitionState(OperationState.READING);
	}

	public final void initialize() {
		if (hasBuiltBuffer())
			throw new IllegalStateException();
	}

	public abstract void readFromBuffer(ByteBuffer data) throws IOException;

	
	protected void handleError(OperationErrorType eType, String line)
		throws IOException {
		getLogger().error("Error:  %s", line);
		OperationException exception = null;
		switch(eType) {
			case GENERAL:
				exception=new OperationException();
				break;
			case SERVER:
				exception=new OperationException(eType, line);
				break;
			case CLIENT:
				exception=new OperationException(eType, line);
				break;
			default: assert false;
		}
		handleError(exception);
	}
	
	protected void handleError(OperationException exception) 
	throws IOException {
		this.exception = exception;
		transitionState(OperationState.COMPLETE);
		throw exception;
	}

	public void handleRead(ByteBuffer data) {
		assert false;
	}

	public ServerNode getHandlingNode() {
		return handlingNode;
	}

	public void setHandlingNode(ServerNode to) {
		handlingNode = to;
	}

}
