package net.spy.nio;

/**
 * Thrown by {@link MemcachedClient} when any internal operations timeout.
 *
 * @author Ray Krueger
 * @see net.spy.memcached.MemcachedClient#setGlobalOperationTimeout(long)
 */
public class OperationTimeoutException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public OperationTimeoutException(String message) {
        super(message);
    }

    public OperationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

}