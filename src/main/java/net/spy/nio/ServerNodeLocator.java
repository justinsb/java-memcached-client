package net.spy.nio;

import java.util.Collection;
import java.util.Iterator;

/**
 * Interface for locating a node by hash value.
 */
public interface ServerNodeLocator {

	/**
	 * Get the primary location for the given key.
	 *
	 * @param k the object key
	 * @return the QueueAttachment containing the primary storage for a key
	 */
	ServerNode getPrimary(String k);

	/**
	 * Get an iterator over the sequence of nodes that make up the backup
	 * locations for a given key.
	 *
	 * @param k the object key
	 * @return the sequence of backup nodes.
	 */
	Iterator<ServerNode> getSequence(String k);

	/**
	 * Get all memcached nodes.  This is useful for broadcasting messages.
	 */
	Collection<ServerNode> getAll();

	/**
	 * Create a read-only copy of this NodeLocator.
	 */
	ServerNodeLocator getReadonlyCopy();
}
