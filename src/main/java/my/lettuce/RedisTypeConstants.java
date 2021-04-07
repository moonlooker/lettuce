package my.lettuce;

/**
 * redis模式
 * @author LL
 *
 */
public interface RedisTypeConstants {
    
    /**
     * 集群
     */
    public final static String CLUSTER = "cluster";
    /**
     * 哨兵模式
     */
    public final static String SENTINEL = "sentinel";
    /**
     * 单机
     */
    public final static String ALONE = "alone";
    /**
     * Lettuce实现
     */
    public final static String INSTANCE_LETTUCE = "lettuce";

}
