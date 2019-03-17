package my.lettuce.base;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

/**
 * 单机连接
 * @author LL
 *
 */
public class LettuceRedisSingle implements LettuceRedis {

    private static Logger log = LoggerFactory.getLogger(LettuceRedisSingle.class);

    private static GenericObjectPool<StatefulRedisConnection<String, String>> pool;

    public LettuceRedisSingle(GenericObjectPoolConfig poolConf, RedisURI redisUri) {

        ClientResources res = DefaultClientResources.builder().computationThreadPoolSize(8).ioThreadPoolSize(4).build();

        RedisClient client = RedisClient.create(res, redisUri);
        pool = ConnectionPoolSupport.createGenericObjectPool(() -> client.connect(), poolConf);
    }

    @Override
    public Long incr(String key) {

        StatefulRedisConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisCommands<String, String> commands = connection.sync();
            long i = commands.incr(key);
            return i;
        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
            //            connection.close();
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }

        return null;
    }

    @Override
    public Long incrWithTTL(String key, int ttl) {

        StatefulRedisConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisCommands<String, String> commands = connection.sync();
            long i = commands.incr(key);
            if (i == 1) {
                commands.expire(key, ttl);
            }
            return i;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("redis操作出错,key:" + key, e);
            //            connection.close();
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }

        return null;
    }

    @Override
    public String save(String key, String value) {

        StatefulRedisConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisCommands<String, String> commands = connection.sync();
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

        StatefulRedisConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisCommands<String, String> commands = connection.sync();
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

        StatefulRedisConnection<String, String> connection = null;
        try {
            connection = pool.borrowObject();
            RedisCommands<String, String> commands = connection.sync();

            return commands.expire(key, ttl);

        } catch (Exception e) {
            log.error("redis操作出错,key:" + key, e);
            //            connection.close();
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }

        return false;
    }

}
