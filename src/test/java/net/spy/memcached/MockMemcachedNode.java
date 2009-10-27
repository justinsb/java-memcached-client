package net.spy.memcached;

import java.net.InetSocketAddress;

import net.spy.nio.MockServerNode;

public class MockMemcachedNode extends MockServerNode implements MemcachedNode {

	public MockMemcachedNode(InetSocketAddress socketAddress) {
		super(socketAddress);
	}

}
