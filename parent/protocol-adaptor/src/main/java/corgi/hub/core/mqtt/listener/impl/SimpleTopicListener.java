package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.ISubscriptionManager;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.common.MqttConstants;
import corgi.hub.core.mqtt.listener.TopicListener;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Terry LIANG on 2017/1/21.
 */
@Component("defaultTopicListener")
public class SimpleTopicListener implements TopicListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTopicListener.class);
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Resource(name = "mqttSubscriptionManager")
    private ISubscriptionManager subscriptionManager;
    private String internalTopic = MqttConstants.INTERNAL_PUBLISH_TOPIC;
    @Autowired
    private HubContext context;
    @Override
    public void onMessage(Object message) {
        try {
            long storeMsgId = (long) message;
            MqttStoreMessage mqttStoreMessage = storageService.retrieveStoreMessage(storeMsgId);
            String topic = mqttStoreMessage.getTopic();
            List<Subscription> onlineSubscriptionList = subscriptionManager.matches(topic);
            LOGGER.info("Start to publish message {} to online client", storeMsgId);
            for(Subscription subscription : onlineSubscriptionList) {
                String clientId = subscription.getClientId();
                String key = mqttStoreMessage.getKey();
                if (!clientId.equals(key) && !MqttUtil.isBroadcastMessage(key)) {
                    LOGGER.debug("This message {} doesn't belong to this client {}", storeMsgId, clientId);
                    continue;
                }
                int qos = mqttStoreMessage.getQos();
                byte[] msg = mqttStoreMessage.getMessage();
                boolean retain = mqttStoreMessage.isRetain();
                if (context.getSession(clientId) == null) {
                    context.getSubscriptionManager().cleanupOfflineSubscriptino(clientId);
                    LOGGER.warn("Client {} offline now", clientId);
                    continue;
                }
                ServerChannel subscribeChannel = context.getSession(clientId).getChannel();
                // Check if client ack the message already
                if (!storageService.alreadyAck(clientId, storeMsgId)) {
                    MqttUtil.publish2Subscribers(subscribeChannel, clientId, mqttStoreMessage.getTopic(), qos, msg, retain, storeMsgId);
                    storageService.ackStoreMessage(clientId, storeMsgId);
                    LOGGER.info("Ack message {} for client {}", storeMsgId, clientId);
                } else {
                    LOGGER.warn("Client {} already received this message {} before", clientId, storeMsgId);
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public String getListenerName() {
        return SimpleTopicListener.class.getName();
    }
}
