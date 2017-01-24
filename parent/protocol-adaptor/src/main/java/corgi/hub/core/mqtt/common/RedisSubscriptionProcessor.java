package corgi.hub.core.mqtt.common;

import com.alibaba.fastjson.JSON;
import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.HubSession;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * Created by Terry LIANG on 2017/1/11.
 */
@Component("redisSubscriptionProcessor")
public class RedisSubscriptionProcessor extends JedisPubSub {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSubscriptionProcessor.class);
    @Autowired
    private JedisConnectionManager jedisConnectionManager;

    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Resource(name = "mqttSubscriptionManager")
    private ISubscriptionManager subscriptionManager;

    @Autowired
    private HubContext context;

    public RedisSubscriptionProcessor() {

    }

    @PostConstruct
    public void init() {
        try {
            Runnable subTask = () -> {
                jedisConnectionManager.getPersistConnection().subscribe(this, MqttConstants.EVENT_TOPIC);
            };
            Runnable subTask2 = () -> {
                jedisConnectionManager.getPersistConnection().subscribe(this, MqttConstants.COMMAND_TOPIC);
            };
            new Thread(subTask).start();
            new Thread(subTask2).start();
        } catch (Exception e) {

        }
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        LOGGER.info("Subscribe {} successfully....", channel);
    }

    /**
     * Process Qos 0 message only
     * @param channelName
     * @param message
     */
    @Override
    public void onMessage(String channelName, String message) {
        subscriptionManager.matches(channelName).stream().forEach(subscription -> {
            MqttPublishEvent mqttPublishEvent = JSON.parseObject(message, MqttPublishEvent.class);
            String clientId = subscription.getClientId();
            int qos = subscription.getRequestedQos();
            qos = qos <= mqttPublishEvent.getQos() ? qos : mqttPublishEvent.getQos();
            byte[] msg = mqttPublishEvent.getMessage();
            boolean retain = mqttPublishEvent.isRetain();
            int msgId = mqttPublishEvent.getMsgId();
            HubSession session = context.getSession(subscription.getClientId());
            if (session != null) {
                ServerChannel subscribeChannel = session.getChannel();
                publish2Subscribers(subscribeChannel, clientId, channelName, qos, msg, retain, msgId);
            }
        });
    }

    private void publish2Subscribers(ServerChannel subscribeChannel, String clientId, String topic, int qos, byte[] message, boolean retain, Integer msgId) {
        if (qos == 0) {
            sendPublishMessageToSubscriber(subscribeChannel, clientId, topic, qos, message, retain);
        } else {
            sendPublishMessageToSubscriber(subscribeChannel, clientId, topic, qos, message, retain, msgId);
        }
    }

    private void sendPublishMessageToSubscriber(ServerChannel subscribeChannel, String clientId, String topic, int qos, byte[] message, boolean retained) {
        sendPublishMessageToSubscriber(subscribeChannel, clientId, topic, qos, message, retained, 0);
    }

    private void sendPublishMessageToSubscriber(ServerChannel subscribeChannel, String clientId, String topic, int qos, byte[] message, boolean retained, int msgId) {
        MqttPublishMessage pubMessage = MqttUtil.createPublishMessage(topic, qos, message, retained, msgId);
        try {
            subscribeChannel.write(pubMessage);
        }catch(Throwable t) {
            t.printStackTrace();
        }
    }

}
