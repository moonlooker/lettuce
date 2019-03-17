package my.lettuce;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import my.lettuce.config.LettuceConfig;

/**
 * 基于Lettuce的Redis客户端
 * @author LL
 *
 */
public class LettuceRedisTest {

    private static Logger log = LoggerFactory.getLogger(LettuceRedisTest.class);

    private static GenericObjectPool<StatefulRedisConnection<String, String>> pool;

    static {
        Config rconf = LettuceConfig.getRedis();
        GenericObjectPoolConfig poolConf = new GenericObjectPoolConfig();
        poolConf.setMaxIdle(rconf.getInt("max.idle"));
        poolConf.setMaxTotal(rconf.getInt("max.active"));
        poolConf.setMaxWaitMillis(rconf.getLong("max.wait"));
        poolConf.setMinIdle(rconf.getInt("min.idle"));
        ClientResources res = DefaultClientResources.builder().computationThreadPoolSize(8).ioThreadPoolSize(4).build();
        
        String server = rconf.getString("server");
        String[] servers = server.split(",");
        String[] t = servers[0].split(":");
        RedisURI redisUri = RedisURI.Builder.redis(t[0], Integer.valueOf(t[1]))
            .withPassword(rconf.getString("auth")).build();
        RedisClient client = RedisClient.create(res, redisUri);
        pool = ConnectionPoolSupport.createGenericObjectPool(() -> client.connect(), poolConf);

    }

    /**
     * 原子增加
     * @param key
     * @return
     */
    public static Long incr(String key) {

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

    /**
     * 设置一个带生存时间的原子增加
     * 从原子技术1开始设置时间
     * @param key
     * @param ttl
     * @return
     */
    public static Long incrWithTTL(String key, int ttl) {

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
            log.error("redis操作出错,key:" + key, e);
            //            connection.close();
        } finally {
            if (connection != null) {
                pool.returnObject(connection);
            }

        }

        return null;
    }

    /**
     * 存储一个K,V
     * @param key
     * @param value
     * @return
     */
    public static String save(String key, String value) {

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

    /**
     * 获取一个K的V
     * @param key
     * @return
     */
    public static String get(String key) {

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
    
    /**
     * 设置一个K的生存时间
     * @param key
     * @param ttl
     * @return
     */
    public static boolean expireKey(String key, long ttl) {

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

        public static void main(String[] args) {
    
                    long a = System.currentTimeMillis();
                    for (int i = 0; i < 10000; i++) {
                        LettuceRedisTest.incrWithTTL("test:aaaeeeddfasea123123123sdf", 10000);
                    }
            
                    System.out.println(System.currentTimeMillis() - a);
    
//            for (int i = 0; i < 100; i++) {
//                new Thread(() -> {
//                    long b = System.currentTimeMillis();
//                    for (int j = 0; j < 1000; j++) {
//                        LettuceRedisTest.incrWithTTL("test:aaaeeeddfasea123123123sdf", 10000);
//                    }
//                    System.out.println(System.currentTimeMillis() - b);
//                }).start();
//            }
//    
//            try {
//                Thread.sleep(100000);
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
            //        
            //        System.out.println(LettuceRedisUtils.get("test"));
        }
}
