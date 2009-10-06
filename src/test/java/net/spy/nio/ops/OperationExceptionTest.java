package net.spy.nio.ops;

import junit.framework.TestCase;

/**
 * Test operation exception constructors and accessors and stuff.
 */
public class OperationExceptionTest extends TestCase {

	public void testEmpty() {
		OperationException oe=new OperationException();
		assertSame(OperationErrorType.GENERAL, oe.getType());
		assertEquals("OperationException: GENERAL", String.valueOf(oe));
	}

	public void testServer() {
		OperationException oe=new OperationException(
			OperationErrorType.SERVER, "SERVER_ERROR figures");
		assertSame(OperationErrorType.SERVER, oe.getType());
		assertEquals("OperationException: SERVER: SERVER_ERROR figures",
			String.valueOf(oe));
	}

	public void testClient() {
		OperationException oe=new OperationException(
			OperationErrorType.CLIENT, "CLIENT_ERROR nope");
		assertSame(OperationErrorType.CLIENT, oe.getType());
		assertEquals("OperationException: CLIENT: CLIENT_ERROR nope",
			String.valueOf(oe));
	}

	public void testGeneral() {
		// General type doesn't have additional info
		OperationException oe=new OperationException(
			OperationErrorType.GENERAL, "GENERAL wtf");
		assertSame(OperationErrorType.GENERAL, oe.getType());
		assertEquals("OperationException: GENERAL", String.valueOf(oe));
	}
}
