package corgi.hub.core.mqtt.bean;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public class RetainedMessage {
    private int qos;
    private byte[] payload;
    private String topic;

    public RetainedMessage() {}

    public RetainedMessage(byte[] message, int qos, String topic) {
        this.payload = message.clone();
        this.qos = qos;
        this.topic = topic;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
