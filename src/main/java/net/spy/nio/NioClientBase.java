package net.spy.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.compat.SpyThread;
import net.spy.nio.ops.Operation;

public abstract class NioClientBase extends SpyThread {
	protected final ServerConnection conn;
	protected final long operationTimeout;

	private volatile boolean running=true;
	private volatile boolean shuttingDown=false;

	public NioClientBase(ConnectionFactory cf, List<InetSocketAddress> addrs) throws IOException {
		if(cf == null) {
			throw new NullPointerException("Connection factory required");
		}
		if(addrs == null) {
			throw new NullPointerException("Server list required");
		}
		if(addrs.isEmpty()) {
			throw new IllegalArgumentException(
				"You must have at least one server to connect to");
		}
		if(cf.getOperationTimeout() <= 0) {
			throw new IllegalArgumentException(
				"Operation timeout must be positive.");
		}
		conn=cf.createConnection(addrs);
		assert conn != null : "Connection factory failed to make a connection";
		setName("Client over " + conn);
		operationTimeout = cf.getOperationTimeout();
		setDaemon(cf.isDaemon());
	}

	/**
	 * Get the addresses of available servers.
	 *
	 * <p>
	 * This is based on a snapshot in time so shouldn't be considered
	 * completely accurate, but is a useful for getting a feel for what's
	 * working and what's not working.
	 * </p>
	 */
	public Collection<SocketAddress> getAvailableServers() {
		Collection<SocketAddress> rv=new ArrayList<SocketAddress>();
		for(ServerNode node : conn.getLocator().getAll()) {
			if(node.isActive()) {
				rv.add(node.getSocketAddress());
			}
		}
		return rv;
	}
	
	/**
	 * Get the addresses of unavailable servers.
	 *
	 * <p>
	 * This is based on a snapshot in time so shouldn't be considered
	 * completely accurate, but is a useful for getting a feel for what's
	 * working and what's not working.
	 * </p>
	 */
	public Collection<SocketAddress> getUnavailableServers() {
		Collection<SocketAddress> rv=new ArrayList<SocketAddress>();
		for(ServerNode node : conn.getLocator().getAll()) {
			if(!node.isActive()) {
				rv.add(node.getSocketAddress());
			}
		}
		return rv;
	}

	protected CountDownLatch broadcastOp(final BroadcastOpFactory of) {
		return broadcastOp(of, true);
	}

	protected CountDownLatch broadcastOp(BroadcastOpFactory of,
			boolean checkShuttingDown) {
		if(checkShuttingDown && shuttingDown) {
			throw new IllegalStateException("Shutting down");
		}
		return conn.broadcastOperation(of);
	}

	protected void checkState() {
		if(shuttingDown) {
			throw new IllegalStateException("Shutting down");
		}
		assert isAlive() : "IO Thread is not running.";
	}

	/**
	 * Add a connection observer.
	 *
	 * @return true if the observer was added.
	 */
	public boolean addObserver(ConnectionObserver obs) {
		return conn.addObserver(obs);
	}

	/**
	 * Remove a connection observer.
	 *
	 * @return true if the observer existed, but no longer does
	 */
	public boolean removeObserver(ConnectionObserver obs) {
		return conn.removeObserver(obs);
	}
	
	private void logRunException(Exception e) {
		if(shuttingDown) {
			// There are a couple types of errors that occur during the
			// shutdown sequence that are considered OK.  Log at debug.
			getLogger().debug("Exception occurred during shutdown", e);
		} else {
			getLogger().warn("Problem handling memcached IO", e);
		}
	}

	/**
	 * Infinitely loop processing IO.
	 */
	@Override
	public void run() {
		while(running) {
			try {
				conn.handleIO();
			} catch(IOException e) {
				logRunException(e);
			} catch(CancelledKeyException e) {
				logRunException(e);
			} catch(ClosedSelectorException e) {
				logRunException(e);
			} catch(IllegalStateException e) {
				logRunException(e);
			}
		}
		getLogger().info("Shut down memcached client");
	}

	/**
	 * Shut down immediately.
	 */
	public void shutdown() {
		shutdown(-1, TimeUnit.MILLISECONDS);
	}

	/**
	 * Shut down this client gracefully.
	 */
	public boolean shutdown(long timeout, TimeUnit unit) {
		// Guard against double shutdowns (bug 8).
		if(shuttingDown) {
			getLogger().info("Suppressing duplicate attempt to shut down");
			return false;
		}
		shuttingDown=true;
		String baseName=getName();
		setName(baseName + " - SHUTTING DOWN");
		boolean rv=false;
		try {
			// Conditionally wait
			if(timeout > 0) {
				setName(baseName + " - SHUTTING DOWN (waiting)");
				rv=waitForQueues(timeout, unit);
			}
		} finally {
			// But always begin the shutdown sequence
			try {
				setName(baseName + " - SHUTTING DOWN (telling client)");
				running=false;
				conn.shutdown();
				setName(baseName + " - SHUTTING DOWN (informed client)");
				shutdownLocalState();
			} catch (IOException e) {
				getLogger().warn("exception while shutting down", e);
			}
		}
		return rv;
	}

	/**
	 * Get a read-only wrapper around the node locator wrapping this instance.
	 */
	public ServerNodeLocator getNodeLocator() {
		return conn.getLocator().getReadonlyCopy();
	}

	/**
	 * (internal use) Add a raw operation to a numbered connection.
	 * This method is exposed for testing.
	 *
	 * @param which server number
	 * @param op the operation to perform
	 * @return the Operation
	 */
	protected Operation addOp(final String key, final Operation op) {
		validateKey(key);
		checkState();
		conn.addOperation(key, op);
		return op;
	}

	protected void addOperations(Map<ServerNode, Operation> multipleOps) {
		conn.addOperations(multipleOps);
	}

	
	protected abstract void validateKey(String key);

	protected abstract boolean waitForQueues(long timeout, TimeUnit unit);

	protected abstract void shutdownLocalState();

	public long getOperationTimeout() {
		return operationTimeout;
	}
}
