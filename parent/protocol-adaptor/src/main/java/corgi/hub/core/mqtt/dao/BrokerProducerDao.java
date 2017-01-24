package corgi.hub.core.mqtt.dao;

import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.event.MqttEvent;
import corgi.hub.core.mqtt.common.IKafkaCallback;

import java.util.List;

/**
 * Created by Terry LIANG on 2017/1/3.
 */
public interface BrokerProducerDao {

    void produceMessageByBatch(HubContext context, List<MqttEvent> mqttEventList);

    void produceMessage(HubContext context, MqttEvent mqttEvent);

    void repushPublishEvent(String publishKey);

    <T extends MqttEvent> void produceMessageWithCallback(HubContext context, T mqttEvent, IKafkaCallback<T> kafkaCallback);

}
