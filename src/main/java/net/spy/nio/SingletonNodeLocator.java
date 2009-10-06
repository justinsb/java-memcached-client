package net.spy.nio;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * NodeLocator for when we only have one server.
 */
public final class SingletonNodeLocator implements ServerNodeLocator {

	final ServerNode node;

	public SingletonNodeLocator(ServerNode node) {
		super();
		this.node = node;
	}

	public Collection<ServerNode> getAll() {
		return Collections.singleton(node);
	}

	public ServerNode getPrimary(String k) {
		return node;
	}

	public Iterator<ServerNode> getSequence(String k) {
		return Collections.singleton(node).iterator();
	}

	public ServerNodeLocator getReadonlyCopy() {
		// I am read-only
		return this;
	}
}
