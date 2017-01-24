package corgi.hub.core.mqtt.dao.impl;

import com.alibaba.fastjson.JSON;
import com.netflix.ribbon.proxy.annotation.ResourceGroup;
import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.IKafkaCallback;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.common.MqttConstants;
import corgi.hub.core.mqtt.dao.BrokerProducerDao;
import corgi.hub.core.mqtt.dao.GenericRedisDao;
import corgi.hub.core.mqtt.event.MqttEvent;
import corgi.hub.core.mqtt.event.MqttPubRelEvent;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.exception.RedisStorageException;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
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
@Component("redisProducerDao")
public class RedisProducerDao extends GenericRedisDao implements BrokerProducerDao {
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisProducerDao.class);
    private String internalPublishQueue = MqttConstants.INTERNAL_PUBLISH_QUEUE;

    @Override
    public void produceMessageByBatch(HubContext context, List<MqttEvent> mqttEventList) {
        try {
            mqttEventList.stream().forEach(event ->{
                MqttPublishEvent mqttPublishEvent = (MqttPublishEvent) event;
                String clientId = mqttPublishEvent.getClientId();
                String redisChannel = MqttUtil.mqttClientTypeString(clientId);
                String payload = JSON.toJSONString(mqttPublishEvent);
                try {
                    if (redisChannel != null) {
                        publish(redisChannel, payload);
                    } else {
                        LOGGER.warn("{} {} Invalid message, skip to publish...", redisChannel, payload);
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void produceMessage(HubContext context, MqttEvent mqttEvent) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            MqttPublishEvent mqttPublishEvent = (MqttPublishEvent) mqttEvent;
            String clientId = mqttPublishEvent.getClientId();
            int msgId = mqttPublishEvent.getMsgId();
            int qos = mqttPublishEvent.getQos();
            String redisChannel = MqttUtil.mqttClientTypeString(clientId);
            String payload = JSON.toJSONString(mqttPublishEvent);
            if (redisChannel != null) {
                if (0 == qos) {
                    publishQos0(redisChannel, payload);
                } else {
                    publishQos1And2(MqttUtil.formatPublishKey(clientId, msgId), payload);
                }
            } else {
                LOGGER.warn("Invalid message, {} {} skip to publish...", redisChannel, payload);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        LOGGER.info("Produce message successfully");
    }

    private void publishQos0(String redisChannel, String payload) throws RedisStorageException {
        publish(redisChannel, payload);
    }

    /**
     * save publishEvent, lpush the publishKey
     * @param publishKey
     * @param message
     * @throws RedisStorageException
     */
    private void publishQos1And2(String publishKey, String message) throws RedisStorageException {
        save(publishKey, message);
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.lpush(internalPublishQueue, publishKey);
            LOGGER.info("Push the publishKey to internal Qos1-2 queue");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    public void repushPublishEvent(String publishKey) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.lpush(internalPublishQueue, publishKey);
            LOGGER.info("Repush the publishKey {} to internal Qos1-2 queue", publishKey);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    private String[] extractSubTopic(String topic) {
        if (topic.contains("/")) {
            return topic.split("/");
        }
        return new String[]{topic, topic};
    }

    @Override
    public <T extends MqttEvent> void produceMessageWithCallback(HubContext context, T mqttEvent, IKafkaCallback<T> kafkaCallback) {
        try {
            MqttPubRelEvent mqttPubRelEvent = (MqttPubRelEvent) mqttEvent;
            String clientId = mqttPubRelEvent.getClientId();
            int msgId = mqttPubRelEvent.getMsgId();
            MqttPublishEvent mqttPublishEvent = (MqttPublishEvent) storageService.retrievePublishedMessage(MqttUtil.formatPublishKey(clientId, msgId));
            produceMessage(context, mqttPublishEvent);
            kafkaCallback.callback(context, mqttEvent);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
