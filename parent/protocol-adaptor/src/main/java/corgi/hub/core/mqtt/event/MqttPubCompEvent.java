package corgi.hub.core.mqtt.event;

/**
 * Created by Terry LIANG on 2016/12/27.
 */
public class MqttPubCompEvent extends BaseEvent implements MqttEvent {
    private int messageId;

    public MqttPubCompEvent() {

    }

    public MqttPubCompEvent(int messageId, String clientId) {
        this.messageId = messageId ;
        this.clientId = clientId;
    }
    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }
}
