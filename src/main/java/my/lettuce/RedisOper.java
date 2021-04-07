package my.lettuce;

import java.util.Map;

/**
 * 指令接口
 * @author LL
 *
 */
public interface RedisOper {
	
	public void close();

    /**
     * 原子增加
     * @param key
     * @return
     */
    public Long incr(String key);
    
    /**
     * 原子增加一個值
     * @param key
     * @param value
     * @return
     */
    public Long incrBy(String key, long value);

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
     * set指令
     * @param key
     * @param value
     * @return String
     */
    public String save(String key, String value);

    /**
     * 获取一个K的V
     * get指令
     * @param key
     * @return String
     */
    public String get(String key);
    
    /**
     * 获取一个Map<String,String>
     * hgetall指令
     * @param key
     * @return
     */
    public Map<String, String> getHMap(String key);
    
    /**
     * 获取Map中的一个值
     * hget指令
     * @param key
     * @param field
     * @return
     */
    public String getHMap(String key, String field);
    
    /**
     *给一个Map添加值
     *hset指令
     * @param key
     * @param field
     * @param value
     * @return
     */
    public boolean setHMap(String key, String field, String value);
    
    public long delHMapField(String key, String field);

    /**
     * 设置一个key的过期时间
     * expire指令
     * @param key
     * @param ttl
     * @return
     */
    public boolean expireKey(String key, long ttl);
    
    public long delKey(String key);
}
