package net.spy.memcached;

import net.spy.nio.FailureMode;

/**
 * Test cancellation in the binary protocol.
 */
public class BinaryCancellationTest extends CancellationBaseCase {

	@Override
	protected void initClient() throws Exception {
		initClient(new BinaryConnectionFactory() {
			@Override
			public FailureMode getFailureMode() {
				return FailureMode.Retry;
			}
		});
	}

}
