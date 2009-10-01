package net.spy.nio.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import net.spy.nio.ServerNode;
import net.spy.nio.ops.Operation;

/**
 * Timeout exception that tracks the original operation.
 */
public class CheckedOperationTimeoutException extends TimeoutException {
	private static final long serialVersionUID = 1L;

	private final Collection<Operation> operations;

	/**
	 * Construct a CheckedOperationTimeoutException with the given message
	 * and operation.
	 *
	 * @param message the message
	 * @param op the operation that timed out
	 */
	public CheckedOperationTimeoutException(String message, Operation op) {
		this(message, Collections.singleton(op));
	}

	public CheckedOperationTimeoutException(String message,
			Collection<Operation> ops) {
		super(createMessage(message, ops));
		operations = ops;
	}

	private static String createMessage(String message,
			Collection<Operation> ops) {
		StringBuilder rv = new StringBuilder(message);
		rv.append(" - failing node");
		rv.append(ops.size() == 1 ? ": " : "s: ");
		boolean first = true;
		for(Operation op : ops) {
			if(first) {
				first = false;
			} else {
				rv.append(", ");
			}
			ServerNode node = op == null ? null : op.getHandlingNode();
			rv.append(node == null ? "<unknown>" : node.getSocketAddress());
		}
		return rv.toString();
	}

	/**
	 * Get the operation that timed out.
	 */
	public Collection<Operation> getOperations() {
		return operations;
	}
}
