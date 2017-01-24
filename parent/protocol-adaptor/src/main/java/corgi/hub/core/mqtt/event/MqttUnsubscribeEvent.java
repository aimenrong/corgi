package corgi.hub.core.mqtt.event;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;

import java.util.List;

/**
 * Created by Terry LIANG on 2016/12/27.
 */
public class MqttUnsubscribeEvent extends BaseEvent implements MqttEvent {
    private int msgId;
    private List<String> topics;

    public MqttUnsubscribeEvent() {

    }

    public MqttUnsubscribeEvent(MqttMessage mqttMessage) {
        MqttUnsubscribePayload payload = (MqttUnsubscribePayload) mqttMessage.payload();
        this.topics = payload.topics();
    }

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }

    public List<String> getTopics() {
        return topics;
    }

}
