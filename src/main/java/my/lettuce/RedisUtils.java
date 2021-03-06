package my.lettuce;

import java.util.Map;



/**
 * 基于Lettuce的Redis客户端
 * @author LL
 *
 */
public class RedisUtils {

//    private static Logger log = LoggerFactory.getLogger(LettuceRedisUtils.class);

    private static RedisOper redisClient;

    static {
        redisClient = RedisInstanceFactory.getRedisOper();
    }

    /**
     * 原子增加
     * @param key
     * @return
     */
    public static Long incr(String key) {

        return redisClient.incr(key);
    }

    /**
     * 设置一个带生存时间的原子增加
     * 从原子技术1开始设置时间
     * @param key
     * @param ttl
     * @return
     */
    public static Long incrWithTTL(String key, int ttl) {

        return redisClient.incrWithTTL(key, ttl);
    }

    /**
     * 存储一个K,V
     * @param key
     * @param value
     * @return
     */
    public static String save(String key, String value) {

        return redisClient.save(key, value);
    }

    /**
     * 获取一个K的V
     * @param key
     * @return
     */
    public static String get(String key) {

        return redisClient.get(key);
    }

    /**
     * 设置一个K的生存时间
     * @param key
     * @param ttl
     * @return
     */
    public static boolean expireKey(String key, long ttl) {

        return redisClient.expireKey(key, ttl);
    }
    
    public static Long incrBy(String key, long value) {
        return redisClient.incrBy(key, value);
    }
    
    public static Map<String, String> getHMap(String key) {
        return redisClient.getHMap(key);
    }
    
    public static boolean setHMap(String key, String field, String value) {
        return redisClient.setHMap(key, field, value);
    }
    
    public static String getHMap(String key, String field) {
        return redisClient.getHMap(key, field);
    }
    
    public static long delHMapField(String key, String field) {
        return redisClient.delHMapField(key, field);
    }
    
//    public static void main(String[] args) {
//        RedisUtils.incrBy("test:aaaeee", 100);
//                long a = System.currentTimeMillis();
//                for (int i = 0; i < 10000; i++) {
//                    RedisUtils.incrWithTTL("test:aaaeeeddfasea123123123sdf", 100);
//                }
//        
//                System.out.println(System.currentTimeMillis() - a);
//
////        for (int i = 0; i < 1; i++) {
////            new Thread(() -> {
////                long b = System.currentTimeMillis();
////                for (int j = 0; j < 1000; j++) {
////                    LettuceRedisUtils.incrWithTTL("test:aaaeeeddfasea123123123sdf", 10000);
////                }
////                System.out.println(System.currentTimeMillis() - b);
////            }).start();
////        }
//
//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        //        
//        //        System.out.println(LettuceRedisUtils.get("test"));
//    }

}
