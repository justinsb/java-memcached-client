package net.spy.memcached.ops;

import java.util.Collection;

import net.spy.nio.ops.Operation;

/**
 * Operations that contain keys.
 */
public interface KeyedOperation extends Operation {

	/**
	 * Get the keys requested in this GetOperation.
	 */
	Collection<String> getKeys();

}
