/**
 *
 */
package net.spy.memcached.internal;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A future that fires immediately.
 */
public class ImmediateFuture implements Future<Boolean> {
	private final Boolean value;
	private final ExecutionException exception;

	public static ImmediateFuture TRUE = new ImmediateFuture(true);
	public static ImmediateFuture FALSE = new ImmediateFuture(false);
	public static ImmediateFuture NULL = new ImmediateFuture((Boolean) null);

	public static ImmediateFuture build(Boolean returnValue) {
		if (returnValue == null)
			return NULL;
		return (returnValue.booleanValue() ? TRUE : FALSE);
	}

	private ImmediateFuture(Boolean returnValue) {
		value = returnValue;
		exception = null;
	}

	public ImmediateFuture(Exception e) {
		value = null;
		exception = new ExecutionException(e);
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public Boolean get() throws InterruptedException, ExecutionException {
		if(exception != null) {
			throw exception;
		}
		return value;
	}

	public Boolean get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException,
			TimeoutException {
		if(exception != null) {
			throw exception;
		}
		return value;
	}

	public boolean isCancelled() {
		return false;
	}

	public boolean isDone() {
		return true;
	}

}