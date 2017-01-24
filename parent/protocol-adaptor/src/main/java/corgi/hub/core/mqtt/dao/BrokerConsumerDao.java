package corgi.hub.core.mqtt.dao;

import corgi.hub.core.mqtt.event.MqttSubscribeEvent;

import java.util.List;

/**
 * Created by Terry LIANG on 2017/1/7.
 */
public interface BrokerConsumerDao {
    void subscribeMessage(MqttSubscribeEvent subscribeEvent);

    boolean topicExist(String clientId, String topic);

    boolean topicExist(String clientId, List<String> topic);

    void cancelConsumer(String clientId, String topic);
}
