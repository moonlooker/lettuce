package my.lettuce.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.typesafe.config.Config;

import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import my.lettuce.config.LettuceConfig;

/**
 * LettuceRedis工厂类
 * 根据配置文件中的type来生成LettuceRedis的实现
 * @author LL
 *
 */
public class LettuceRedisFactory {

    private LettuceRedisFactory() {

    };

    public static LettuceRedis getLettuceRedis() {

        Config rconf = LettuceConfig.getRedis();
        GenericObjectPoolConfig poolConf = new GenericObjectPoolConfig();
        poolConf.setMaxIdle(rconf.getInt("max.idle"));
        poolConf.setMaxTotal(rconf.getInt("max.active"));
        poolConf.setMaxWaitMillis(rconf.getLong("max.wait"));
        poolConf.setMinIdle(rconf.getInt("min.idle"));

        String type = rconf.getString("type");
        
        if (RedisTypeConstants.CLUSTER.equals(type)) {
            /*如果是cluster模式*/
            return new LettuceRedisCluster(poolConf, splitServerHost(rconf));                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
        } else if (RedisTypeConstants.SENTINEL.equals(type)) {
            /*如果是哨兵模式*/
            return new LettuceRedisSingle(poolConf, splitSentinelServerHost(rconf));
        } else {
            /*单机模式*/
            return new LettuceRedisSingle(poolConf, splitServerHost(rconf).get(0));
        }
    }

    /**
     * 拆分配置
     * @param rconf
     * @return
     */
    private static List<RedisURI> splitServerHost(Config rconf) {

        String server = rconf.getString("server");
        String auth = rconf.getString("auth");
        String[] servers = server.split(",");

        List<RedisURI> result = new ArrayList<>();

        for (String host : servers) {
            String[] temp = host.split(":");

            if (auth != null && !"".equals(auth)) {
                RedisURI redisUri = RedisURI.Builder.redis(temp[0], Integer.valueOf(temp[1]))
                    .withPassword(rconf.getString("auth")).build();
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
     * @param rconf
     * @return
     */
    private static RedisURI splitSentinelServerHost(Config rconf) {
        Builder builder = RedisURI.builder();
        
        String server = rconf.getString("server");
        String auth = rconf.getString("auth");
        String mName = rconf.getString("master.name");
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
