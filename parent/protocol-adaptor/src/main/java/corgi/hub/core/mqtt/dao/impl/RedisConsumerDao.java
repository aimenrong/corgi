package corgi.hub.core.mqtt.dao.impl;

import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.dao.BrokerConsumerDao;
import corgi.hub.core.mqtt.event.MqttSubscribeEvent;
import corgi.hub.core.mqtt.service.IStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by Terry LIANG on 2017/1/11.
 */
@Component("redisConsumerDao")
public class RedisConsumerDao implements BrokerConsumerDao {
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConsumerDao.class);

    @Autowired
    private JedisConnectionManager jedisConnectionManager;

    @Override
    public void subscribeMessage(MqttSubscribeEvent subscribeEvent) {
        String subscriberClientId = subscribeEvent.getClientId();
        Jedis jedis = null;
        try {

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean topicExist(String clientId, String topic) {
        return false;
    }

    @Override
    public boolean topicExist(String clientId, List<String> topic) {
        return false;
    }

    @Override
    public void cancelConsumer(String clientId, String topic) {

    }
}
