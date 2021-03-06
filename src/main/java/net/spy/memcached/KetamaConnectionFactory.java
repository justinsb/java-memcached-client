package net.spy.memcached;

import java.util.List;

import net.spy.nio.DefaultConnectionFactory;
import net.spy.nio.HashAlgorithm;
import net.spy.nio.KetamaNodeLocator;
import net.spy.nio.ServerNode;
import net.spy.nio.ServerNodeLocator;

/**
 * ConnectionFactory instance that sets up a ketama compatible connection.
 *
 * <p>
 * This implementation piggy-backs on the functionality of the
 * <code>DefaultConnectionFactory</code> in terms of connections and queue
 * handling. Where it differs is that it uses both the <code>
 * KetamaNodeLocator</code> and the <code>HashAlgorithm.KETAMA_HASH</code>
 * to provide consistent node hashing.
 *
 * @see http://www.last.fm/user/RJ/journal/2007/04/10/392555/
 *
 * </p>
 */
public class KetamaConnectionFactory extends DefaultMemcachedConnectionFactory {
	/**
	 * Create a KetamaConnectionFactory with the given maximum operation
	 * queue length, and the given read buffer size.
	 */
	public KetamaConnectionFactory(int qLen, int bufSize) {
		super(qLen, bufSize, HashAlgorithm.KETAMA_HASH);
	}

	/**
	 * Create a KetamaConnectionFactory with the default parameters.
	 */
	public KetamaConnectionFactory() {
		this(DEFAULT_OP_QUEUE_LEN, DEFAULT_READ_BUFFER_SIZE);
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.ConnectionFactory#createLocator(java.util.List)
	 */
	@Override
	public ServerNodeLocator createLocator(List<? extends ServerNode> nodes) {
		return new KetamaNodeLocator(nodes, getHashAlg());
	}
}
