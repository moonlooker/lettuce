package my.lettuce;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.typesafe.config.Config;

import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import my.lettuce.base.LettuceRedisClusterOper;
import my.lettuce.base.LettuceRedisSingleOper;
import my.lettuce.base.LettuceRedisSinglePoolOper;
import my.lettuce.config.LettuceConfig;

/**
 * LettuceRedis工厂类 根据配置文件中的type来生成LettuceRedis的实现
 * 
 * @author LL
 */
public class RedisInstanceFactory {

	private RedisInstanceFactory() {

	};

	public static RedisOper getRedisOper( Config rconf) {

		return createRedisOper(rconf);
	}

	public static RedisOper getRedisOper() {

		Config rconf = LettuceConfig.getRedis();
		return createRedisOper(rconf);
	}

	private static RedisOper createRedisOper( Config rconf) {

		GenericObjectPoolConfig poolConf = new GenericObjectPoolConfig();
		poolConf.setMaxIdle(rconf.getInt("pool.max.idle"));
		poolConf.setMaxTotal(rconf.getInt("pool.max.active"));
		poolConf.setMaxWaitMillis(rconf.getLong("pool.max.wait"));
		poolConf.setMinIdle(rconf.getInt("pool.min.idle"));

		String type = rconf.getString("client.type");
		String instance = rconf.getString("client.instance");
		boolean usePool = rconf.getBoolean("client.use.pool");

		if (RedisTypeConstants.CLUSTER.equals(type)) {
			/*如果是cluster模式*/
			return new LettuceRedisClusterOper(poolConf, splitServerHost(rconf));
		} else if (RedisTypeConstants.SENTINEL.equals(type)) {
			/*如果是哨兵模式*/
			return new LettuceRedisSinglePoolOper(poolConf, splitSentinelServerHost(rconf));
		} else {
			if (RedisTypeConstants.INSTANCE_LETTUCE.equals(instance)) {
				if (usePool) {
					/*单机模式*/
					return new LettuceRedisSinglePoolOper(poolConf, splitServerHost(rconf).get(0));
				} else {
					return new LettuceRedisSingleOper(poolConf, splitServerHost(rconf).get(0));
				}
			}
		}
		return null;
	}

	/**
	 * 拆分配置
	 * 
	 * @param rconf
	 * @return
	 */
	private static List<RedisURI> splitServerHost( Config rconf) {

		String server = rconf.getString("client.server");
		String auth = rconf.getString("client.auth");
		String[] servers = server.split(",");

		List<RedisURI> result = new ArrayList<>();

		for (String host : servers) {
			String[] temp = host.split(":");

			if (auth != null && !"".equals(auth)) {
				RedisURI redisUri = RedisURI.Builder.redis(temp[0], Integer.valueOf(temp[1])).withPassword(auth)
						.build();
				result.add(redisUri);
			} else {
				RedisURI redisUri = RedisURI.Builder.redis(temp[0], Integer.valueOf(temp[1])).build();
				result.add(redisUri);
			}
		}

		return result;
	}

	/**
	 * 拆分Sentinel配置
	 * 
	 * @param rconf
	 * @return
	 */
	private static RedisURI splitSentinelServerHost( Config rconf) {

		Builder builder = RedisURI.builder();

		String server = rconf.getString("client.server");
		String auth = rconf.getString("client.auth");
		String mName = rconf.getString("client.master.name");
		String[] servers = server.split(",");

		builder.withSentinelMasterId(mName);

		for (String host : servers) {
			String[] temp = host.split(":");
			builder.withSentinel(temp[0], Integer.valueOf(temp[1]));
		}

		if (auth != null && !"".equals(auth)) {
			builder.withPassword(auth);
		}

		return builder.build();
	}

}
