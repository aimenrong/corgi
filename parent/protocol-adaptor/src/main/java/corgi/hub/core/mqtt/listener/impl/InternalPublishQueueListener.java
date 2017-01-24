package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.common.MqttConstants;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.exception.RedisStorageException;
import corgi.hub.core.mqtt.listener.QueueListener;
import corgi.hub.core.mqtt.service.IQueueService;
import corgi.hub.core.mqtt.service.IStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * Created by Terry LIANG on 2017/1/19.
 */
@Component("InternalPublishQueueListener")
public class InternalPublishQueueListener extends AbstractQueueListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(InternalPublishQueueListener.class);
    private String name = InternalPublishQueueListener.class.getName();

    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Autowired
    private IQueueService queueService;
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    private static final String EXCLUSIVE_QUEUE_PREFIX = "exclusiveQueue-";
    private String internalTopic;
    @Autowired
    private HubContext context;

    @PostConstruct
    private void init() {
//        this.exclusiveQueue = EXCLUSIVE_QUEUE_PREFIX + "-" + context.getNodeName();
        internalTopic = MqttConstants.INTERNAL_PUBLISH_TOPIC;
    }

    @Override
    public void onQueueMessage(Object message) {
        String publishKey = (String) message;
        MqttPublishEvent mqttPublishEvent = (MqttPublishEvent) storageService.retrievePublishedMessage(publishKey);
        Jedis jedis = null;
        try {
            if (mqttPublishEvent != null) {
                jedis = jedisConnectionManager.getPersistConnection();
                MqttStoreMessage mqttStoreMessage = storageService.addStoreMessage(mqttPublishEvent);
                long storeMsgId = mqttStoreMessage.getStoreMsgId();
                storageService.removePublishedMessage(publishKey);
                String topic = mqttPublishEvent.getTopic();
                // Add the logic for downlink message to specify device
                if (mqttStoreMessage.getKey() != null && !"*".equals(mqttStoreMessage.getKey())) {
                    Subscription subscription = storageService.retrieveSubscription(mqttStoreMessage.getKey(), topic);
                    String exclusiveQueue = EXCLUSIVE_QUEUE_PREFIX + "-" + subscription.getNodeBelongTo();
                    jedis.lpush(exclusiveQueue, Long.toString(storeMsgId));
                } else if (topic.startsWith("VirtualTopic")) {
                    queueService.pushQueueMessage(topic, storeMsgId);
                } else {
                    storageService.loggingNotifyEvent(storeMsgId);
                    notifySubscriber(Long.toString(storeMsgId));
                }
            } else {

            }
        } catch (RedisStorageException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private void notifySubscriber(String message) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.publish(internalTopic, message);
        } catch (Exception e) {
            throw new RedisStorageException(String.format("Fail to publish %s %s... ", internalTopic, message), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public String getListenerName() {
        return this.name;
    }
}
