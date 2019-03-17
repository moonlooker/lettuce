package my.lettuce.base;

/**
 * 指令接口
 * @author LL
 *
 */
public interface LettuceRedis {

    /**
     * 原子增加
     * @param key
     * @return
     */
    public Long incr(String key);

    /**
     * 带过期时间的原子增加,
     * 过期时间在incr=1的时候才会设置一次
     * @param key
     * @param ttl
     * @return
     */
    public Long incrWithTTL(String key, int ttl);

    /**
     * 存储一个K,V
     * @param key
     * @param value
     * @return String
     */
    public String save(String key, String value);

    /**
     * 获取一个K的V
     * @param key
     * @return String
     */
    public String get(String key);

    /**
     * 设置一个key的过期时间
     * @param key
     * @param ttl
     * @return
     */
    public boolean expireKey(String key, long ttl);
}
