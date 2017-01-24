package corgi.hub.core.mqtt.bean;

import java.util.Date;

/**
 * Created by Terry LIANG on 2017/1/12.
 */
public class MqttStoreMessage {
    private long storeMsgId;
    // Default is broadcast message
    private String key = "*";
    private String clientId;
    private String topic;
    private int msgId;
    private int qos;
    private byte[] message;
    private boolean retain;
    private long createTime;

    public long getStoreMsgId() {
        return storeMsgId;
    }

    public void setStoreMsgId(long storeMsgId) {
        this.storeMsgId = storeMsgId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}
