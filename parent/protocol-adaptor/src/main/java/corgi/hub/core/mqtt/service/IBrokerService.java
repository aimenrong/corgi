package corgi.hub.core.mqtt.service;

import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.event.MqttEvent;

/**
 * Created by Terry LIANG on 2017/1/3.
 */
public interface IBrokerService {

    void processConnect(HubContext context, MqttEvent mqttEvent);

    void processDisconnect(HubContext context, MqttEvent mqttEvent);

    void processSubscribe(HubContext context, MqttEvent mqttEvent);

    void processUnsubscribe(HubContext context, MqttEvent mqttEvent);

    void processPubRec(HubContext context, MqttEvent mqttEvent);

    void processPubRel(HubContext context, MqttEvent mqttEvent);

    void processPubAck(HubContext context, MqttEvent mqttEvent);

    void processPubComp(HubContext context, MqttEvent mqttEvent);

    void processPublish(HubContext context, MqttEvent mqttEvent);

}
