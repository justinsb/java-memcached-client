// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.nio;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import net.spy.memcached.compat.SpyObject;
import net.spy.nio.ops.Operation;
import net.spy.nio.ops.OperationState;

/**
 * Connection to a cluster of memcached servers.
 */
public abstract class ServerConnection extends SpyObject {

	// The number of empty selects we'll allow before assuming we may have
	// missed one and should check the current selectors.  This generally
	// indicates a bug, but we'll check it nonetheless.
	private static final int DOUBLE_CHECK_EMPTY = 256;
	// The number of empty selects we'll allow before blowing up.  It's too
	// easy to write a bug that causes it to loop uncontrollably.  This helps
	// find those bugs and often works around them.
	private static final int EXCESSIVE_EMPTY = 0x1000000;
	// maximum amount of time to wait between reconnect attempts
	private static final long MAX_DELAY = 30000;

	private volatile boolean shutDown=false;
	// If true, optimization will collapse multiple sequential get ops
	private final boolean shouldOptimize;
	private Selector selector=null;
	private final ServerNodeLocator locator;
	private final FailureMode failureMode;
	private int emptySelects=0;
	// AddedQueue is used to track the QueueAttachments for which operations
	// have recently been queued.
	private final ConcurrentLinkedQueue<ServerNode> addedQueue;
	// reconnectQueue contains the attachments that need to be reconnected
	// The key is the time at which they are eligible for reconnect
	private final SortedMap<Long, ServerNode> reconnectQueue;

	private final Collection<ConnectionObserver> connObservers =
		new ConcurrentLinkedQueue<ConnectionObserver>();
	
	/**
	 * Construct a memcached connection.
	 *
	 * @param bufSize the size of the buffer used for reading from the server
	 * @param f the factory that will provide an operation queue
	 * @param a the addresses of the servers to connect to
	 *
	 * @throws IOException if a connection attempt fails early
	 */
	public ServerConnection(int bufSize, ConnectionFactory f,
			List<InetSocketAddress> a, Collection<ConnectionObserver> obs,
			FailureMode fm)
		throws IOException {
		connObservers.addAll(obs);
		reconnectQueue=new TreeMap<Long, ServerNode>();
		addedQueue=new ConcurrentLinkedQueue<ServerNode>();
		failureMode = fm;
		shouldOptimize = f.shouldOptimize();
		selector=Selector.open();
		List<ServerNode> connections=new ArrayList<ServerNode>(a.size());
		for(SocketAddress sa : a) {
			SocketChannel ch=SocketChannel.open();
			ch.configureBlocking(false);
			ServerNode qa=f.createServerNode(sa, ch, bufSize);
			int ops=0;
			ch.socket().setTcpNoDelay(!f.useNagleAlgorithm());
			// Initially I had attempted to skirt this by queueing every
			// connect, but it considerably slowed down start time.
			try {
				if(ch.connect(sa)) {
					getLogger().info("Connected to %s immediately", qa);
					connected(qa);
				} else {
					getLogger().info("Added %s to connect queue", qa);
					ops=SelectionKey.OP_CONNECT;
				}
				qa.setSk(ch.register(selector, ops, qa));
				assert ch.isConnected()
					|| qa.getSk().interestOps() == SelectionKey.OP_CONNECT
					: "Not connected, and not wanting to connect";
			} catch(ConnectException e) {
				queueReconnect(qa);
			}
			connections.add(qa);
		}
		locator=f.createLocator(connections);
	}

	private boolean selectorsMakeSense() {
		for(ServerNode qa : locator.getAll()) {
			if(qa.getSk() != null && qa.getSk().isValid()) {
				if(qa.getChannel().isConnected()) {
					int sops=qa.getSk().interestOps();
					int expected=0;
					if(qa.hasReadOp()) {
						expected |= SelectionKey.OP_READ;
					}
					if(qa.hasWriteOp()) {
						expected |= SelectionKey.OP_WRITE;
					}
					if(qa.getBytesRemainingToWrite() > 0) {
						expected |= SelectionKey.OP_WRITE;
					}
					assert sops == expected : "Invalid ops:  "
						+ qa + ", expected " + expected + ", got " + sops;
				} else {
					int sops=qa.getSk().interestOps();
					assert sops == SelectionKey.OP_CONNECT
					: "Not connected, and not watching for connect: "
						+ sops;
				}
			}
		}
		getLogger().debug("Checked the selectors.");
		return true;
	}

	/**
	 * MemcachedClient calls this method to handle IO over the connections.
	 */
	public void handleIO() throws IOException {
		if(shutDown) {
			throw new IOException("No IO while shut down");
		}

		// Deal with all of the stuff that's been added, but may not be marked
		// writable.
		handleInputQueue();
		getLogger().debug("Done dealing with queue.");

		long delay=0;
		if(!reconnectQueue.isEmpty()) {
			long now=System.currentTimeMillis();
			long then=reconnectQueue.firstKey();
			delay=Math.max(then-now, 1);
		}
		getLogger().debug("Selecting with delay of %sms", delay);
		assert selectorsMakeSense() : "Selectors don't make sense.";
		int selected=selector.select(delay);
		Set<SelectionKey> selectedKeys=selector.selectedKeys();

		if(selectedKeys.isEmpty() && !shutDown) {
			getLogger().debug("No selectors ready, interrupted: "
					+ Thread.interrupted());
			if(++emptySelects > DOUBLE_CHECK_EMPTY) {
				for(SelectionKey sk : selector.keys()) {
					getLogger().info("%s has %s, interested in %s",
							sk, sk.readyOps(), sk.interestOps());
					if(sk.readyOps() != 0) {
						getLogger().info("%s has a ready op, handling IO", sk);
						handleIO(sk);
					} else {
						lostConnection((ServerNode)sk.attachment());
					}
				}
				assert emptySelects < EXCESSIVE_EMPTY
					: "Too many empty selects";
			}
		} else {
			getLogger().debug("Selected %d, selected %d keys",
					selected, selectedKeys.size());
			emptySelects=0;
			for(SelectionKey sk : selectedKeys) {
				handleIO(sk);
			} // for each selector
			selectedKeys.clear();
		}

		if(!shutDown && !reconnectQueue.isEmpty()) {
			attemptReconnects();
		}
	}

	// Handle any requests that have been made against the client.
	private void handleInputQueue() {
		if(!addedQueue.isEmpty()) {
			getLogger().debug("Handling queue");
			// If there's stuff in the added queue.  Try to process it.
			Collection<ServerNode> toAdd=new HashSet<ServerNode>();
			// Transfer the queue into a hashset.  There are very likely more
			// additions than there are nodes.
			Collection<ServerNode> todo=new HashSet<ServerNode>();
			try {
				ServerNode qa=null;
				while((qa=addedQueue.remove()) != null) {
					todo.add(qa);
				}
			} catch(NoSuchElementException e) {
				// Found everything
			}

			// Now process the queue.
			for(ServerNode qa : todo) {
				boolean readyForIO=false;
				if(qa.isActive()) {
					if(qa.getCurrentWriteOp() != null) {
						readyForIO=true;
						getLogger().debug("Handling queued write %s", qa);
					}
				} else {
					toAdd.add(qa);
				}
				qa.copyInputQueue();
				if(readyForIO) {
					try {
						if(qa.getWbuf().hasRemaining()) {
							handleWrites(qa.getSk(), qa);
						}
					} catch(IOException e) {
						getLogger().warn("Exception handling write", e);
						lostConnection(qa);
					}
				}
				qa.fixupOps();
			}
			addedQueue.addAll(toAdd);
		}
	}

	/**
	 * Add a connection observer.
	 *
	 * @return whether the observer was successfully added
	 */
	public boolean addObserver(ConnectionObserver obs) {
		return connObservers.add(obs);
	}

	/**
	 * Remove a connection observer.
	 *
	 * @return true if the observer existed and now doesn't
	 */
	public boolean removeObserver(ConnectionObserver obs) {
		return connObservers.remove(obs);
	}

	private void connected(ServerNode qa) {
		assert qa.getChannel().isConnected() : "Not connected.";
		int rt = qa.getReconnectCount();
		qa.connected();
		for(ConnectionObserver observer : connObservers) {
			observer.connectionEstablished(qa.getSocketAddress(), rt);
		}
	}

	private void lostConnection(ServerNode qa) {
		queueReconnect(qa);
		for(ConnectionObserver observer : connObservers) {
			observer.connectionLost(qa.getSocketAddress());
		}
	}

	// Handle IO for a specific selector.  Any IOException will cause a
	// reconnect
	private void handleIO(SelectionKey sk) {
		ServerNode qa=(ServerNode)sk.attachment();
		try {
			getLogger().debug(
					"Handling IO for:  %s (r=%s, w=%s, c=%s, op=%s)",
					sk, sk.isReadable(), sk.isWritable(),
					sk.isConnectable(), sk.attachment());
			if(sk.isConnectable()) {
				getLogger().info("Connection state changed for %s", sk);
				final SocketChannel channel=qa.getChannel();
				if(channel.finishConnect()) {
					connected(qa);
					addedQueue.offer(qa);
					if(qa.getWbuf().hasRemaining()) {
						handleWrites(sk, qa);
					}
				} else {
					assert !channel.isConnected() : "connected";
				}
			} else {
				if(sk.isReadable()) {
					handleReads(sk, qa);
				}
				if(sk.isWritable()) {
					handleWrites(sk, qa);
				}
			}
		} catch(ClosedChannelException e) {
			if(!shutDown) {
				getLogger().info("Closed channel and not shutting down.  "
					+ "Queueing reconnect on %s", qa, e);
				lostConnection(qa);
			}
		} catch(ConnectException e) {
			// Failures to establish a connection should attempt a reconnect
			// without signaling the observers.
			getLogger().info("Reconnecting due to failure to connect to %s",
					qa, e);
			queueReconnect(qa);
		} catch(Exception e) {
			// Various errors occur on Linux that wind up here.  However, any
			// particular error processing an item should simply cause us to
			// reconnect to the server.
			getLogger().info("Reconnecting due to exception on %s", qa, e);
			lostConnection(qa);
		}
		qa.fixupOps();
	}

	private void handleWrites(SelectionKey sk, ServerNode qa)
		throws IOException {
		qa.fillWriteBuffer(shouldOptimize);
		boolean canWriteMore=qa.getBytesRemainingToWrite() > 0;
		while(canWriteMore) {
			int wrote=qa.writeSome();
			qa.fillWriteBuffer(shouldOptimize);
			canWriteMore = wrote > 0 && qa.getBytesRemainingToWrite() > 0;
		}
	}

	private void handleReads(SelectionKey sk, ServerNode qa)
		throws IOException {
		Operation currentOp = qa.getCurrentReadOp();
		ByteBuffer rbuf=qa.getRbuf();
		final SocketChannel channel = qa.getChannel();
		int read=channel.read(rbuf);
		if (read < 0) {
		    // GRUMBLE.
		    throw new IOException("Disconnected");
		}
		while(read > 0) {
			getLogger().debug("Read %d bytes", read);
			rbuf.flip();
			while(rbuf.remaining() > 0) {
				if(currentOp == null) {
					throw new IllegalStateException("No read operation.");
				}
				currentOp.readFromBuffer(rbuf);
				if(currentOp.getState() == OperationState.COMPLETE) {
					getLogger().debug(
							"Completed read op: %s and giving the next %d bytes",
							currentOp, rbuf.remaining());
					Operation op=qa.removeCurrentReadOp();
					assert op == currentOp
					: "Expected to pop " + currentOp + " got " + op;
					currentOp=qa.getCurrentReadOp();
				}
			}
			rbuf.clear();
			read=channel.read(rbuf);
		}
	}

	// Make a debug string out of the given buffer's values
	static String dbgBuffer(ByteBuffer b, int size) {
		StringBuilder sb=new StringBuilder();
		byte[] bytes=b.array();
		for(int i=0; i<size; i++) {
			char ch=(char)bytes[i];
			if(Character.isWhitespace(ch) || Character.isLetterOrDigit(ch)) {
				sb.append(ch);
			} else {
				sb.append("\\x");
				sb.append(Integer.toHexString(bytes[i] & 0xff));
			}
		}
		return sb.toString();
	}

	private void queueReconnect(ServerNode qa) {
		if(!shutDown) {
			getLogger().warn("Closing, and reopening %s, attempt %d.",
					qa, qa.getReconnectCount());
			if(qa.getSk() != null) {
				qa.getSk().cancel();
				assert !qa.getSk().isValid() : "Cancelled selection key is valid";
			}
			qa.reconnecting();
			try {
				if(qa.getChannel() != null && qa.getChannel().socket() != null) {
					qa.getChannel().socket().close();
				} else {
					getLogger().info("The channel or socket was null for %s",
						qa);
				}
			} catch(IOException e) {
				getLogger().warn("IOException trying to close a socket", e);
			}
			qa.setChannel(null);

			long delay=Math.min((100*qa.getReconnectCount()) ^ 2, MAX_DELAY);
			long reconTime = System.currentTimeMillis() + delay;

			// Avoid potential condition where two connections are scheduled
			// for reconnect at the exact same time.  This is expected to be
			// a rare situation.
			while(reconnectQueue.containsKey(reconTime)) {
				reconTime++;
			}

			reconnectQueue.put(reconTime, qa);

			// Need to do a little queue management.
			qa.setupResend();

			if(failureMode == FailureMode.Redistribute) {
				redistributeOperations(qa.destroyInputQueue());
			} else if(failureMode == FailureMode.Cancel) {
				cancelOperations(qa.destroyInputQueue());
			}
		}
	}

	protected abstract void redistributeOperations(Collection<Operation> ops);

	private void cancelOperations(Collection<Operation> ops) {
		for(Operation op : ops) {
			op.cancel();
		}
	}

	
	private void attemptReconnects() throws IOException {
		final long now=System.currentTimeMillis();
		final Map<ServerNode, Boolean> seen=
			new IdentityHashMap<ServerNode, Boolean>();
		final List<ServerNode> rereQueue=new ArrayList<ServerNode>();
		for(Iterator<ServerNode> i=
				reconnectQueue.headMap(now).values().iterator(); i.hasNext();) {
			final ServerNode qa=i.next();
			i.remove();
			try {
				if(!seen.containsKey(qa)) {
					seen.put(qa, Boolean.TRUE);
					getLogger().info("Reconnecting %s", qa);
					final SocketChannel ch=SocketChannel.open();
					ch.configureBlocking(false);
					int ops=0;
					if(ch.connect(qa.getSocketAddress())) {
						getLogger().info("Immediately reconnected to %s", qa);
						assert ch.isConnected();
					} else {
						ops=SelectionKey.OP_CONNECT;
					}
					qa.registerChannel(ch, ch.register(selector, ops, qa));
					assert qa.getChannel() == ch : "Channel was lost.";
				} else {
					getLogger().debug(
						"Skipping duplicate reconnect request for %s", qa);
				}
			} catch(ConnectException e) {
				getLogger().warn("Error on reconnect", e);
				rereQueue.add(qa);
			}
		}
		// Requeue any fast-failed connects.
		for(ServerNode n : rereQueue) {
			queueReconnect(n);
		}
	}

	/**
	 * Get the node locator used by this connection.
	 */
	public ServerNodeLocator getLocator() {
		return locator;
	}

	/**
	 * Add an operation to the given connection.
	 *
	 * @param which the connection offset
	 * @param o the operation
	 */
	public void addOperation(final String key, final Operation o) {
		ServerNode placeIn=null;
		ServerNode primary = locator.getPrimary(key);
		if(primary.isActive() || failureMode == FailureMode.Retry) {
			placeIn=primary;
		} else if(failureMode == FailureMode.Cancel) {
			o.cancel();
		} else {
			// Look for another node in sequence that is ready.
			for(Iterator<ServerNode> i=locator.getSequence(key);
				placeIn == null && i.hasNext(); ) {
				ServerNode n=i.next();
				if(n.isActive()) {
					placeIn=n;
				}
			}
			// If we didn't find an active node, queue it in the primary node
			// and wait for it to come back online.
			if(placeIn == null) {
				placeIn = primary;
			}
		}

		assert o.isCancelled() || placeIn != null
			: "No node found for key " + key;
		if(placeIn != null) {
			addOperation(placeIn, o);
		} else {
			assert o.isCancelled() : "No not found for "
				+ key + " (and not immediately cancelled)";
		}
	}

	public void addOperation(final ServerNode node, final Operation o) {
		o.setHandlingNode(node);
		o.initialize();
		node.addOp(o);
		addedQueue.offer(node);
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
		getLogger().debug("Added %s to %s", o, node);
	}

	public void addOperations(final Map<ServerNode, Operation> ops) {

		for(Map.Entry<ServerNode, Operation> me : ops.entrySet()) {
			final ServerNode node=me.getKey();
			Operation o=me.getValue();
			o.setHandlingNode(node);
			o.initialize();
			node.addOp(o);
			addedQueue.offer(node);
		}
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
	}

	/**
	 * Broadcast an operation to all nodes.
	 */
	public CountDownLatch broadcastOperation(final BroadcastOpFactory of) {
		final CountDownLatch latch=new CountDownLatch(locator.getAll().size());
		for(ServerNode node : locator.getAll()) {
			Operation op = of.newOp(node, latch);
			op.initialize();
			node.addOp(op);
			op.setHandlingNode(node);
			addedQueue.offer(node);
		}
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
		return latch;
	}

	/**
	 * Shut down all of the connections.
	 */
	public void shutdown() throws IOException {
		shutDown=true;
		Selector s=selector.wakeup();
		assert s == selector : "Wakeup returned the wrong selector.";
		for(ServerNode qa : locator.getAll()) {
			if(qa.getChannel() != null) {
				qa.getChannel().close();
				qa.setSk(null);
				if(qa.getBytesRemainingToWrite() > 0) {
					getLogger().warn(
						"Shut down with %d bytes remaining to write",
							qa.getBytesRemainingToWrite());
				}
				getLogger().debug("Shut down channel %s", qa.getChannel());
			}
		}
		selector.close();
		getLogger().debug("Shut down selector %s", selector);
	}

	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append("{ServerConnection to");
		for(ServerNode qa : locator.getAll()) {
			sb.append(" ");
			sb.append(qa.getSocketAddress());
		}
		sb.append("}");
		return sb.toString();
	}

}
