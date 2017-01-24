package corgi.hub.core.mqtt.event;

import corgi.hub.core.mqtt.ServerChannel;

import java.util.Date;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public class MqttPublishEvent extends BaseEvent implements MqttEvent {
    private String topic;
    private int qos;
    private byte[] message;
    private boolean retain;
    private long createTime = new Date().getTime();
    private String receiverId;

    //Optional attribute, available only fo QoS 1 and 2
    private int msgId;

    public MqttPublishEvent() {

    }

    public MqttPublishEvent(String topic, int qos, byte[] message, boolean retain,
                            String clientId, ServerChannel channel) {
        this.topic = topic;
        this.qos = qos;
        this.message = message.clone();
        this.retain = retain;
        this.clientId = clientId;
        this.channel = channel;
    }

    public MqttPublishEvent(String topic, int qos, byte[] message, boolean retain,
                            String clientId, int msgId, ServerChannel channel) {
        this(topic, qos, message, retain, clientId, channel);
        this.msgId = msgId;
    }

    public String getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(String receiverId) {
        this.receiverId = receiverId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public byte[] getMessage() {
        return message == null ? null : message.clone();
    }

    public void setMessage(byte[] message) {
        this.message = message == null ? null : message.clone();
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "MqttPublishEvent{" +
                "m_msgID=" + msgId +
                ", m_clientID='" + clientId + '\'' +
                ", m_retain=" + retain +
                ", m_qos=" + qos +
                ", createTime=" + createTime +
                ", m_topic='" + topic + '\'' +
                '}';
    }
}
