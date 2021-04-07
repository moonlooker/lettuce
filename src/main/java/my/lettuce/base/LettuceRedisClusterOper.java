package my.lettuce.base;

import java.util.List;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import my.lettuce.RedisOper;

/**
 * cluster模式
 * @author LL
 *
 */
public class LettuceRedisClusterOper implements RedisOper {

    private static Logger log = LoggerFactory.getLogger(LettuceRedisClusterOper.class);

    private static GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;

    public LettuceRedisClusterOper(GenericObjectPoolConfig poolConf, List<RedisURI> result) {

//        ClientResources res = DefaultClientResources.builder().computationThreadPoolSize(8).ioThreadPoolSize(4).build();
    	ClientResources res = DefaultClientResources.builder().build();
        RedisClusterClient client = RedisClusterClient.create(res, result);
        pool = ConnectionPoolSupport.createGenericObjectPool(() -> client.connect(), poolConf);
    }

    @Override
    public Long incr(String key) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            long i = commands.incr(key);
            return i;
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }

        return null;
    }

    @Override
    public Long incrWithTTL(String key, int ttl) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            long i = commands.incr(key);
            if (i == 1) {
                commands.expire(key, ttl);
            }
            return i;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }

        return null;
    }

    @Override
    public String save(String key, String value) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            return commands.set(key, value);
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }
        return null;
    }

    @Override
    public String get(String key) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            return commands.get(key);
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }
        return null;
    }

    @Override
    public boolean expireKey(String key, long ttl) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();

            return commands.expire(key, ttl);

        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }

        return false;
    }

    @Override
    public Long incrBy(String key, long value) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            long i = commands.incrby(key, value);
            return i;
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }

        return null;
    }

    @Override
    public Map<String, String> getHMap(String key) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            return commands.hgetall(key);
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }
        }
        return null;
    }

    @Override
    public boolean setHMap(String key, String field, String value) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            return commands.hset(key, field, value);
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }
        return false;
    }

    @Override
    public String getHMap(String key, String field) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            return commands.hget(key, field);
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }
        }
        return null;
    }

    @Override
    public long delHMapField(String key, String field) {


        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            return commands.hdel(key, field);
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }
        }
        return 0;
    }

    @Override
    public long delKey(String key) {

        StatefulRedisClusterConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisAdvancedClusterCommands<String, String> commands = connection.sync();
            return commands.del(key);
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }
        }
        return 0;
    }

	@Override
	public void close() {
		pool.close();
	}

}
