package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.bean.VirtualTopicConsumerShard;
import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.exception.NoConsumerAvailableException;
import corgi.hub.core.mqtt.listener.QueueListener;
import corgi.hub.core.mqtt.service.IQueueShardingService;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Terry LIANG on 2017/1/19.
 */
@Component("VirtualTopicQueueListener")
public class VirtualTopicQueueListener implements QueueListener, IQueueShardingService {
    private final Logger LOGGER = LoggerFactory.getLogger(VirtualTopicQueueListener.class);

    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Autowired
    private HubContext context;
    private Map<String, VirtualTopicConsumerShard> virtualTopicConsumerShardMap = new ConcurrentHashMap<>();

    @Override
    public void onQueueMessage(Object message) {
        try {
            long storeMsgId = (long)message;
            MqttStoreMessage mqttStoreMessage = (MqttStoreMessage) storageService.retrieveStoreMessage(storeMsgId);
            if (mqttStoreMessage != null) {
                String topic = mqttStoreMessage.getTopic();
                VirtualTopicConsumerShard virtualTopicConsumerShard = virtualTopicConsumerShardMap.get(topic);
                if (null == virtualTopicConsumerShard) {
                    throw new NoConsumerAvailableException("No available consumer, cannot ack this message");
                }
                int shardSize = virtualTopicConsumerShard.size();
                Subscription subscription = virtualTopicConsumerShard.keyToNode(Long.toString(storeMsgId % shardSize));
                if (null == subscription) {
                    throw new NoConsumerAvailableException("No available consumer, cannot ack this message");
                }
                if (context.getSession(subscription.getClientId()) != null) {
                    ServerChannel serverChannel = context.getSession(subscription.getClientId()).getChannel();
                    MqttPublishMessage mqttPublishMessage = MqttUtil.createPublishMessage(mqttStoreMessage.getTopic(), mqttStoreMessage.getQos(),
                            mqttStoreMessage.getMessage(), mqttStoreMessage.isRetain(), mqttStoreMessage.getMsgId());
                    serverChannel.write(mqttPublishMessage);
                }
            } else {

            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void putShard(String queue, Subscription subscription) {
        if (virtualTopicConsumerShardMap.containsKey(queue)) {
            virtualTopicConsumerShardMap.get(queue).addS(subscription);
        } else {
            Set<Subscription> set = new ConcurrentSet<>();
            set.add(subscription);
            VirtualTopicConsumerShard virtualTopicConsumerShard = new VirtualTopicConsumerShard(set);
            virtualTopicConsumerShardMap.put(queue, virtualTopicConsumerShard);
        }
    }

    @Override
    public Subscription removeShard(String queue, Subscription subscription) {
        if (virtualTopicConsumerShardMap.containsKey(queue)) {
            VirtualTopicConsumerShard virtualTopicConsumerShard = virtualTopicConsumerShardMap.get(queue);
            virtualTopicConsumerShard.deleteS(subscription);
            if (virtualTopicConsumerShard.size() == 1) {
                virtualTopicConsumerShardMap.remove(queue);
            }
        }
        return subscription;
    }

    @Override
    public int size(String queue) {
        if (virtualTopicConsumerShardMap.containsKey(queue)) {
            VirtualTopicConsumerShard virtualTopicConsumerShard = virtualTopicConsumerShardMap.get(queue);
            return virtualTopicConsumerShard.size();
        }
        return 0;
    }

    @Override
    public void setListenerName(String name) {

    }

    @Override
    public String getListenerName() {
        return null;
    }
}
