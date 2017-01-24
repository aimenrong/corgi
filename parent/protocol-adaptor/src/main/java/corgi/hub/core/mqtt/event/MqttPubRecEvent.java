package corgi.hub.core.mqtt.event;

/**
 * Created by Terry LIANG on 2016/12/27.
 */
public class MqttPubRecEvent extends BaseEvent implements MqttEvent {
    //Optional attribute, available only fo QoS 1 and 2
    private int msgId;

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }

}
