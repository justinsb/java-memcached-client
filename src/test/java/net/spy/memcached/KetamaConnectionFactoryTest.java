package net.spy.memcached;

import java.util.ArrayList;

import net.spy.nio.ConnectionFactory;
import net.spy.nio.HashAlgorithm;
import net.spy.nio.KetamaNodeLocator;
import net.spy.nio.ServerNodeLocator;

import junit.framework.TestCase;

/**
 * A very basic test that the KetamaConnectionFactory returns both the correct
 * hash algorithm and the correct node locator.
 */
public class KetamaConnectionFactoryTest extends TestCase {

	/*
	 * This *is* kinda lame, but it tests the specific differences from the
	 * DefaultConnectionFactory.
	 */
	public void testCorrectTypes() {
		ConnectionFactory factory = new KetamaConnectionFactory();

		ServerNodeLocator locator = factory.createLocator(new ArrayList<MemcachedNode>());
		assertTrue(locator instanceof KetamaNodeLocator);

		assertEquals(HashAlgorithm.KETAMA_HASH, factory.getHashAlg());
	}
}
