package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import net.spy.nio.ConnectionFactory;
import net.spy.nio.ConnectionObserver;
import net.spy.nio.FailureMode;
import net.spy.nio.ServerConnection;

public class MemcachedConnection extends ServerConnection {

	public MemcachedConnection(int bufSize, ConnectionFactory f, List<InetSocketAddress> a, Collection<ConnectionObserver> obs, FailureMode fm, OperationFactory opfactory) throws IOException {
		super(bufSize, f, a, obs, fm, opfactory);
	}

}
