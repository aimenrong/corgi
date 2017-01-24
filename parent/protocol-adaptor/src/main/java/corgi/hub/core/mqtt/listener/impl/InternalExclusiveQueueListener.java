package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.listener.QueueListener;
import corgi.hub.core.mqtt.service.IQueueService;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * Created by Terry LIANG on 2017/1/19.
 */
@Component("InternalExclusiveQueueListener")
public class InternalExclusiveQueueListener extends AbstractQueueListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(InternalExclusiveQueueListener.class);
    private String name = InternalPublishQueueListener.class.getName();
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Autowired
    private IQueueService queueService;
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    @Autowired
    private HubContext context;

    @Override
    public void onQueueMessage(Object message) {
       try {
           long storeMsgId = (long) message;
           MqttStoreMessage mqttStoreMessage = storageService.retrieveStoreMessage(storeMsgId);
           String receiverId = mqttStoreMessage.getKey();
           if (context.getSession(receiverId) != null) {
               ServerChannel serverChannel = context.getSession(receiverId).getChannel();
               MqttPublishMessage mqttPublishMessage = MqttUtil.createPublishMessage(mqttStoreMessage.getTopic(), mqttStoreMessage.getQos(),
                       mqttStoreMessage.getMessage(), mqttStoreMessage.isRetain(), mqttStoreMessage.getMsgId());
               serverChannel.write(mqttPublishMessage);
               storageService.removeStoreMessage(storeMsgId);
           } else {
               LOGGER.warn("{} not online", receiverId);
           }
       } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
       }
    }

    @Override
    public void setListenerName(String name) {

    }

    @Override
    public String getListenerName() {
        return this.name;
    }
}
