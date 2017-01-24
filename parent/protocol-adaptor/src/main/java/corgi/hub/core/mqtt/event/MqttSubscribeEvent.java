package corgi.hub.core.mqtt.event;

import corgi.hub.core.mqtt.common.HubContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public class MqttSubscribeEvent extends BaseEvent implements MqttEvent {
    private transient HubContext context;
    public static class Couple {
        private int qos;
        private String topic;

        public Couple(int qos, String topic) {
            this.qos = qos;
            this.topic = topic;
        }

        public int getQos() {
            return qos;
        }

        public String getTopic() {
            return topic;
        }
    }
    private List<Couple> subscriptions = new ArrayList<Couple>();
    private int messageId;
    private boolean earliest;

    public MqttSubscribeEvent() {
    }

    public MqttSubscribeEvent(MqttMessage mqttMessage) {
        this.mqttMessage = mqttMessage;
        MqttSubscribePayload payload = (MqttSubscribePayload) mqttMessage.payload();
        List<MqttTopicSubscription> subs = payload.topicSubscriptions();
        for (MqttTopicSubscription sub : subs) {
            Couple couple = new Couple(sub.qualityOfService().value(), sub.topicName());
            subscriptions.add(couple);
        }
    }

    public List<Couple> subscriptions() {
        return subscriptions;
    }

    public void addSubscription(Couple subscription) {
        subscriptions.add(subscription);
    }

    public void setMqttMessage(MqttMessage mqttMessage) {
        this.mqttMessage = mqttMessage;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public boolean isEarliest() {
        return earliest;
    }

    public void setEarliest(boolean earliest) {
        this.earliest = earliest;
    }

    public HubContext getContext() {
        return context;
    }

    public void setContext(HubContext context) {
        this.context = context;
    }
}
