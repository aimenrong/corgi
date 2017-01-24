package corgi.hub.core.mqtt.event;

/**
 * Created by Terry LIANG on 2016/12/25.
 */
public class MqttPubAckEvent extends BaseEvent implements MqttEvent {
    private int msgId;

    public MqttPubAckEvent() {
    }

    public MqttPubAckEvent(int msgId) {
        this.msgId = msgId ;
    }

    public MqttPubAckEvent(int msgId, String clientId) {
        this.msgId = msgId ;
        super.clientId = clientId;
    }

    public int getMessageId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }
}
