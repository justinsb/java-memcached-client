package net.spy.nio;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * NodeLocator implementation for dealing with simple array lookups using a
 * modulus of the hash code and node list length.
 */
public final class ArrayModNodeLocator implements ServerNodeLocator {

	final ServerNode[] nodes;

	private final HashAlgorithm hashAlg;

	/**
	 * Construct an ArraymodNodeLocator over the given array of nodes and
	 * using the given hash algorithm.
	 *
	 * @param n the array of nodes
	 * @param alg the hash algorithm
	 */
	public ArrayModNodeLocator(List<ServerNode> n, HashAlgorithm alg) {
		super();
		nodes=n.toArray(new ServerNode[n.size()]);
		hashAlg=alg;
	}

	private ArrayModNodeLocator(ServerNode[] n, HashAlgorithm alg) {
		super();
		nodes=n;
		hashAlg=alg;
	}

	public Collection<ServerNode> getAll() {
		return Arrays.asList(nodes);
	}

	public ServerNode getPrimary(String k) {
		return nodes[getServerForKey(k)];
	}

	public Iterator<ServerNode> getSequence(String k) {
		return new NodeIterator(getServerForKey(k));
	}

	public ServerNodeLocator getReadonlyCopy() {
		ServerNode[] n=new ServerNode[nodes.length];
		for(int i=0; i<nodes.length; i++) {
			n[i] = new ServerNodeROImpl(nodes[i]);
		}
		return new ArrayModNodeLocator(n, hashAlg);
	}

	private int getServerForKey(String key) {
		int rv=(int)(hashAlg.hash(key) % nodes.length);
		assert rv >= 0 : "Returned negative key for key " + key;
		assert rv < nodes.length
			: "Invalid server number " + rv + " for key " + key;
		return rv;
	}

	class NodeIterator implements Iterator<ServerNode> {

		private final int start;
		private int next=0;

		public NodeIterator(int keyStart) {
			start=keyStart;
			next=start;
			computeNext();
			assert next >= 0 || nodes.length == 1
				: "Starting sequence at " + start + " of "
					+ nodes.length + " next is " + next;
		}

		public boolean hasNext() {
			return next >= 0;
		}

		private void computeNext() {
			if(++next >= nodes.length) {
				next=0;
			}
			if(next == start) {
				next=-1;
			}
		}

		public ServerNode next() {
			try {
				return nodes[next];
			} finally {
				computeNext();
			}
		}

		public void remove() {
			throw new UnsupportedOperationException("Can't remove a node");
		}

	}
}
