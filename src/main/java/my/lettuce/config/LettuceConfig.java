package my.lettuce.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 处理配置文件
 * @author LL
 *
 */
public class LettuceConfig {

    private static Config root = loadConfig();

    private static Config loadConfig() {

        return ConfigFactory.parseResources("lettuce.conf");

    }

    public static Config getRedis() {

        return root.getConfig("redis");
    }

}
