package net.spy.nio;

import java.util.Iterator;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public abstract class AbstractNodeLocationCase extends MockObjectTestCase {

	protected ServerNode[] nodes;
	protected Mock[] nodeMocks;
	protected ServerNodeLocator locator;

	private void runSequenceAssertion(ServerNodeLocator l, String k, int... seq) {
		int pos=0;
		for(Iterator<ServerNode> i=l.getSequence(k); i.hasNext(); ) {
			assertEquals("At position " + pos, nodes[seq[pos]].toString(),
				i.next().toString());
			try {
				i.remove();
				fail("Allowed a removal from a sequence.");
			} catch(UnsupportedOperationException e) {
				// pass
			}
			pos++;
		}
		assertEquals("Incorrect sequence size for " + k, seq.length, pos);
	}

	public final void testCloningGetPrimary() {
		setupNodes(5);
		assertTrue(locator.getReadonlyCopy().getPrimary("hi")
			instanceof ServerNodeROImpl);
	}

	public final void testCloningGetAll() {
		setupNodes(5);
		assertTrue(locator.getReadonlyCopy().getAll().iterator().next()
			instanceof ServerNodeROImpl);
	}

	public final void testCloningGetSequence() {
		setupNodes(5);
		assertTrue(locator.getReadonlyCopy().getSequence("hi").next()
			instanceof ServerNodeROImpl);
	}

	protected final void assertSequence(String k, int... seq) {
		runSequenceAssertion(locator, k, seq);
		runSequenceAssertion(locator.getReadonlyCopy(), k, seq);
	}

	protected void setupNodes(int n) {
		nodes=new ServerNode[n];
		nodeMocks=new Mock[nodes.length];

		for(int i=0; i<nodeMocks.length; i++) {
			nodeMocks[i]=mock(ServerNode.class, "node#" + i);
			nodes[i]=(ServerNode)nodeMocks[i].proxy();
		}
	}
}