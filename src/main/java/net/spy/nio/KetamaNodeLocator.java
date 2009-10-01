package net.spy.nio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.spy.memcached.compat.SpyObject;
import net.spy.nio.HashAlgorithm;
import net.spy.nio.ServerNodeLocator;

/**
 * This is an implementation of the Ketama consistent hash strategy from
 * last.fm.  This implementation may not be compatible with libketama as
 * hashing is considered separate from node location.
 *
 * Note that this implementation does not currently supported weighted nodes.
 *
 * @see http://www.last.fm/user/RJ/journal/2007/04/10/392555/
 */
public final class KetamaNodeLocator extends SpyObject implements ServerNodeLocator {

	static final int NUM_REPS = 160;

	final SortedMap<Long, ServerNode> ketamaNodes;
	final Collection<ServerNode> allNodes;

	final HashAlgorithm hashAlg;

	public KetamaNodeLocator(List<ServerNode> nodes, HashAlgorithm alg) {
		super();
		allNodes = nodes;
		hashAlg = alg;
		ketamaNodes=new TreeMap<Long, ServerNode>();

		for(ServerNode node : nodes) {
			// XXX:  Replace getSocketAddress() with something more precise
			String sockStr=String.valueOf(node.getSocketAddress());
			// Ketama does some special work with md5 where it reuses chunks.
			if(alg == HashAlgorithm.KETAMA_HASH) {
				for(int i=0; i<NUM_REPS / 4; i++) {
					byte[] digest=HashAlgorithm.computeMd5(sockStr + "-" + i);
					for(int h=0;h<4;h++) {
						Long k = ((long)(digest[3+h*4]&0xFF) << 24)
							| ((long)(digest[2+h*4]&0xFF) << 16)
							| ((long)(digest[1+h*4]&0xFF) << 8)
							| (digest[h*4]&0xFF);
						ketamaNodes.put(k, node);
					}

				}
			} else {
				for(int i=0; i<NUM_REPS; i++) {
					ketamaNodes.put(hashAlg.hash(sockStr + "-" + i), node);
				}
			}
		}
		assert ketamaNodes.size() == NUM_REPS * nodes.size();
	}

	private KetamaNodeLocator(SortedMap<Long, ServerNode> smn,
			Collection<ServerNode> an, HashAlgorithm alg) {
		super();
		ketamaNodes=smn;
		allNodes=an;
		hashAlg=alg;
	}

	public Collection<ServerNode> getAll() {
		return allNodes;
	}

	public ServerNode getPrimary(final String k) {
		ServerNode rv=getNodeForKey(hashAlg.hash(k));
		assert rv != null : "Found no node for key " + k;
		return rv;
	}

	long getMaxKey() {
		return ketamaNodes.lastKey();
	}

	ServerNode getNodeForKey(long hash) {
		final ServerNode rv;
		if(!ketamaNodes.containsKey(hash)) {
			// Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
			// in a lot of places, so I'm doing this myself.
			SortedMap<Long, ServerNode> tailMap=ketamaNodes.tailMap(hash);
			if(tailMap.isEmpty()) {
				hash=ketamaNodes.firstKey();
			} else {
				hash=tailMap.firstKey();
			}
		}
		rv=ketamaNodes.get(hash);
		return rv;
	}

	public Iterator<ServerNode> getSequence(String k) {
		return new KetamaIterator(k, allNodes.size());
	}

	public ServerNodeLocator getReadonlyCopy() {
		SortedMap<Long, ServerNode> smn=new TreeMap<Long, ServerNode>(
			ketamaNodes);
		Collection<ServerNode> an=
			new ArrayList<ServerNode>(allNodes.size());

		// Rewrite the values a copy of the map.
		for(Map.Entry<Long, ServerNode> me : smn.entrySet()) {
			me.setValue(new ServerNodeROImpl(me.getValue()));
		}
		// Copy the allNodes collection.
		for(ServerNode n : allNodes) {
			an.add(new ServerNodeROImpl(n));
		}

		return new KetamaNodeLocator(smn, an, hashAlg);
	}

	class KetamaIterator implements Iterator<ServerNode> {

		final String key;
		long hashVal;
		int remainingTries;
		int numTries=0;

		public KetamaIterator(final String k, final int t) {
			super();
			hashVal=hashAlg.hash(k);
			remainingTries=t;
			key=k;
		}

		private void nextHash() {
			// this.calculateHash(Integer.toString(tries)+key).hashCode();
			long tmpKey=hashAlg.hash((numTries++) + key);
			// This echos the implementation of Long.hashCode()
			hashVal += (int)(tmpKey ^ (tmpKey >>> 32));
			hashVal &= 0xffffffffL; /* truncate to 32-bits */
			remainingTries--;
		}

		public boolean hasNext() {
			return remainingTries > 0;
		}

		public ServerNode next() {
			try {
				return getNodeForKey(hashVal);
			} finally {
				nextHash();
			}
		}

		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}

	}
}
