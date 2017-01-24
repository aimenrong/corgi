package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.common.*;
import corgi.hub.core.mqtt.listener.TopicListenable;
import corgi.hub.core.mqtt.listener.TopicListener;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPubSub;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Terry LIANG on 2017/1/21.
 */
@Component("SimpleTopicListenable")
public class SimpleTopicListenable extends JedisPubSub implements TopicListenable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTopicListenable.class);
    protected Map<String, TopicListener> topicListenerMap = new HashMap<>();
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Resource(name = "mqttSubscriptionManager")
    private ISubscriptionManager subscriptionManager;
    private String internalTopic = MqttConstants.INTERNAL_PUBLISH_TOPIC;
    @Autowired
    private HubContext context;
    @Resource(name = "defaultTopicListener")
    private TopicListener defaultTopicListener;
    private Lock lock = new ReentrantLock();

    @PostConstruct
    public void init() {
        try {
            Runnable subTask = () -> {
                jedisConnectionManager.getPersistConnection().subscribe(this, internalTopic);
            };
            new Thread(subTask).start();
            topicListenerMap.put(defaultTopicListener.getListenerName(), defaultTopicListener);
        } catch (Exception e) {

        }
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        LOGGER.info("{} subscribe successfully....", channel);
    }

    /**
     * Process Qos 1 and 2 message only
     */
    @Override
    public void onMessage(String channelName, String message) {
        if (null == message) {
            return;
        }
        try {
            lock.lock();
            long storeMsgId = Long.parseLong(message);
            for (Map.Entry<String, TopicListener> entry : topicListenerMap.entrySet()) {
                entry.getValue().onMessage(storeMsgId);
            }
            // Ack the pub/sub message here
            storageService.ackNotifyEvent(storeMsgId);
        } finally {
            lock.unlock();
        }

        System.out.println(channelName + "=" + message);
    }

    @Override
    public void registerListener(TopicListener topicListener) {
        try {
            lock.lock();
            topicListenerMap.put(topicListener.getListenerName(), topicListener);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unregisterListener(String listenerName) {
        try {
            lock.lock();
            topicListenerMap.remove(listenerName);
        } finally {
            lock.unlock();
        }
    }
}
