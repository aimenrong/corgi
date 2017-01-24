package corgi.hub.core.mqtt.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;

import javax.annotation.PostConstruct;

/**
 * Created by Terry LIANG on 2016/12/18.
 */
@Component
public class JedisConnectionManager {
    private JedisPool persistPool;
    @Value("${app.persist.jedis.maxTotal}")
    private int persistMaxTotal;
    @Value("${app.persist.jedis.maxIdle}")
    private int persistMaxIdle;
    @Value("${app.persist.jedis.pool.host}")
    private String persistJedisPoolHost;
    @Value("${app.persist.jedis.pool.port}")
    private int persistPort;
    @Value("${app.persist.jedis.test.on.borrow}")
    private boolean persistTestOnBorrow;

    @PostConstruct
    public void init() {
        if (persistPool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(persistMaxTotal);
            config.setMaxIdle(persistMaxIdle);
            config.setTestOnBorrow(persistTestOnBorrow);
            persistPool = new JedisPool(config, persistJedisPoolHost, persistPort);
        }
    }

    @Bean(name = "jedisPoolConfig")
    public JedisPoolConfig getJedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(persistMaxTotal);
        jedisPoolConfig.setMaxIdle(persistMaxIdle);
        jedisPoolConfig.setTestOnBorrow(persistTestOnBorrow);
        return jedisPoolConfig;
    }

    @Bean
    public JedisShardInfo getJedisShardInfo() {
        JedisShardInfo jedisShardInfo = new JedisShardInfo(persistJedisPoolHost, persistPort);
        return jedisShardInfo;
    }

    @Bean(name = "jedisConnectionFactory")
    public JedisConnectionFactory getJedisConnectionFactory() {
        JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
        jedisConnectionFactory.setHostName(persistJedisPoolHost);
        jedisConnectionFactory.setPort(persistPort);
        jedisConnectionFactory.setPoolConfig(getJedisPoolConfig());
        jedisConnectionFactory.setShardInfo(getJedisShardInfo());
        return jedisConnectionFactory;
    }

    @Bean(name = "redisTemplate")
    public RedisTemplate getRedisTemplate() {
        RedisTemplate redisTemplate = new RedisTemplate();
        redisTemplate.setConnectionFactory(getJedisConnectionFactory());
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setEnableTransactionSupport(false);
        return redisTemplate;
    }

    @Bean(name = "storeMessageIdGenerator")
    public RedisAtomicLong getRedisAtomicLong() {
        RedisAtomicLong redisAtomicLong = new RedisAtomicLong("storeMessageCounter", getJedisConnectionFactory());
        return redisAtomicLong;
    }

    public Jedis getPersistConnection() {
        return persistPool.getResource();
    }
}
