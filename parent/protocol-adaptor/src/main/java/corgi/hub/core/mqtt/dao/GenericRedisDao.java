package corgi.hub.core.mqtt.dao;

import com.alibaba.fastjson.JSON;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.exception.RedisStorageException;
import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.Jedis;

/**
 * Created by Terry LIANG on 2017/1/11.
 */
public abstract class GenericRedisDao {
    @Autowired
    protected JedisConnectionManager jedisConnectionManager;

    public void save(String tableKey, String tableValue) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.set(tableKey, tableValue);
        } catch (Exception e) {
            throw new RedisStorageException("Fail to save...", e);
        } finally {
            jedis.close();
        }
    }

    public void del(String key) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.del(key);
        } catch (Exception e) {
            throw new RedisStorageException("Fail to delete...", e);
        } finally {
            jedis.close();
        }
    }

    public Object get(String key, Class clazz) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String result =  jedis.get(key);
            Object object = JSON.parseObject(result, clazz);
            return object;
        } catch (Exception e) {
            throw new RedisStorageException("Fail to get... ", e);
        } finally {
            jedis.close();
        }
    }

    public void publish(String channel, String message) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.publish(channel, message);
        } catch (Exception e) {
            throw new RedisStorageException(String.format("Fail to publish %s %s... ", channel, message), e);
        } finally {
            jedis.close();
        }
    }


}
