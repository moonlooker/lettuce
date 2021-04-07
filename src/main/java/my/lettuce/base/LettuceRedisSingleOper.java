package my.lettuce.base;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import my.lettuce.RedisOper;

/**
 * 单机连接
 * 
 * @author LL
 */
public class LettuceRedisSingleOper implements RedisOper {

	private static Logger log = LoggerFactory.getLogger(LettuceRedisSingleOper.class);

	private static GenericObjectPool<StatefulRedisConnection<String, String>> pool;

	private static StatefulRedisConnection<String, String> connection;

	private static RedisClient client;

	public LettuceRedisSingleOper( GenericObjectPoolConfig poolConf, RedisURI redisUri) {
		client = RedisClient.create(redisUri);
		connection = client.connect();
	}

	@Override
	public Long incr( String key) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			long i = commands.incr(key);
			return i;
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return null;
	}

	@Override
	public Long incrWithTTL( String key, int ttl) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			long i = commands.incr(key);
			if (i == 1) {
				commands.expire(key, ttl);
			}
			return i;
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return null;
	}

	@Override
	public String save( String key, String value) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			return commands.set(key, value);
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return null;
	}

	@Override
	public String get( String key) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			return commands.get(key);
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return null;
	}

	@Override
	public boolean expireKey( String key, long ttl) {

		try {
			RedisCommands<String, String> commands = connection.sync();

			return commands.expire(key, ttl);

		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return false;
	}

	@Override
	public Long incrBy( String key, long value) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			long i = commands.incrby(key, value);
			return i;
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return null;
	}

	@Override
	public Map<String, String> getHMap( String key) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			return commands.hgetall(key);
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return null;
	}

	@Override
	public boolean setHMap( String key, String field, String value) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			return commands.hset(key, field, value);
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return false;
	}

	@Override
	public String getHMap( String key, String field) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			return commands.hget(key, field);
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return null;
	}

	@Override
	public long delHMapField( String key, String field) {

		try {
			connection = pool.borrowObject();
			RedisCommands<String, String> commands = connection.sync();
			return commands.hdel(key, field);
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return 0;
	}

	@Override
	public long delKey( String key) {

		try {
			RedisCommands<String, String> commands = connection.sync();
			return commands.del(key);
		} catch ( Exception e) {
			log.error("redis操作出错,key:" + key, e);
		}

		return 0;
	}

	@Override
	public void close() {
		pool.close();
	}

}
