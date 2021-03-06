package net.spy.nio;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import net.spy.memcached.AddrUtil;

import junit.framework.TestCase;

/**
 */
public class ConsistentHashingTest extends TestCase {

	public void testSmallSet() {
		runThisManyNodes(3);
	}

	public void testLargeSet() {
		runThisManyNodes(100);
	}

	/**
	 * Simulate dropping from (totalNodes) to (totalNodes-1).
	 * Ensure hashing is consistent between the the two scenarios.
	 * @param totalNodes
	 */
	private void runThisManyNodes(final int totalNodes) {
		final String[] stringNodes = generateAddresses(totalNodes);

		List<ServerNode> smaller = createNodes(
			AddrUtil.getAddresses(stringNodes[0]));
		List<ServerNode> larger = createNodes(
			AddrUtil.getAddresses(stringNodes[1]));

		assertTrue(larger.containsAll(smaller));
		ServerNode oddManOut = larger.get(larger.size()-1);
		assertFalse(smaller.contains(oddManOut));

		KetamaNodeLocator lgLocator = new KetamaNodeLocator(
			larger, HashAlgorithm.KETAMA_HASH);
		KetamaNodeLocator smLocator = new KetamaNodeLocator(
			smaller, HashAlgorithm.KETAMA_HASH);

		SortedMap<Long, ServerNode> lgMap = lgLocator.ketamaNodes;
		SortedMap<Long, ServerNode> smMap = smLocator.ketamaNodes;

		// Verify that EVERY entry in the smaller map has an equivalent
		// mapping in the larger map.
		boolean failed = false;
		for (final Long key : smMap.keySet()) {
			final ServerNode largeNode = lgMap.get(key);
			final ServerNode smallNode = smMap.get(key);
			if (!largeNode.equals(smallNode)) {
				failed = true;
				System.out.println("---------------");
				System.out.println("Key: " + key);
				System.out.println("Small: " + smallNode.getSocketAddress());
				System.out.println("Large: " + largeNode.getSocketAddress());
			}
		}
		assertFalse(failed);

		for (final Map.Entry<Long, ServerNode> entry : lgMap.entrySet()) {
			final Long key = entry.getKey();
			final ServerNode node = entry.getValue();
			if (node.equals(oddManOut)) {
				final ServerNode newNode = smLocator.getNodeForKey(key);
				if (!smaller.contains(newNode)) {
					System.out.println(
						"Error - " + key + " -> " + newNode.getSocketAddress());
					failed = true;
				}
			}
		}
		assertFalse(failed);

	}

	private String[] generateAddresses(final int maxSize) {
		final String[] results = new String[2];

		// Generate a pseudo-random set of addresses.
		long now = new Date().getTime();
		int first = (int) ((now % 250) + 3);

		int second = (int) (((now/250) % 250) + 3);

		String port = ":11211 ";
		int last = (int) ((now % 100) + 3);

		StringBuffer prefix = new StringBuffer();
		prefix.append(first);
		prefix.append(".");
		prefix.append(second);
		prefix.append(".1.");

		// Don't protect the possible range too much, as we are our own client.
		StringBuffer buf = new StringBuffer();
		for (int ix = 0; ix < maxSize - 1; ix++) {
			buf.append(prefix);
			buf.append(last+ix);
			buf.append(port);
		}

		results[0] = buf.toString();

		buf.append(prefix);
		buf.append(last+maxSize-1);
		buf.append(port);

		results[1] = buf.toString();

		return results;
	}

	private List<ServerNode> createNodes(List<InetSocketAddress> addresses) {
		List<ServerNode> results = new ArrayList<ServerNode>();

		for (InetSocketAddress addr : addresses) {
			results.add(new MockServerNode(addr));
		}

		return results;
	}

}
