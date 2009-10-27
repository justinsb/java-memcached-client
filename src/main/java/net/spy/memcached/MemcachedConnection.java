package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import net.spy.memcached.ops.KeyedOperation;
import net.spy.nio.ConnectionFactory;
import net.spy.nio.ConnectionObserver;
import net.spy.nio.FailureMode;
import net.spy.nio.ServerConnection;
import net.spy.nio.ops.Operation;

public class MemcachedConnection extends ServerConnection {
	private final OperationFactory opFact;
	
	public MemcachedConnection(int bufSize, ConnectionFactory f, List<InetSocketAddress> a, Collection<ConnectionObserver> obs, FailureMode fm, OperationFactory opfactory) throws IOException {
		super(bufSize, f, a, obs, fm);
		
		this.opFact = opfactory;
	}
	
	protected void redistributeOperations(Collection<Operation> ops) {
			for(Operation op : ops) {
				if(op instanceof KeyedOperation) {
					KeyedOperation ko = (KeyedOperation)op;
					int added = 0;
					for(String k : ko.getKeys()) {
						for(Operation newop : opFact.clone(ko)) {
							addOperation(k, newop);
							added++;
						}
					}
					assert added > 0
						: "Didn't add any new operations when redistributing";
				} else {
					// Cancel things that don't have definite targets.
					op.cancel();
				}
			}
		}


}
