package corgi.hub.core.mqtt.bean;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public class Subscription implements Serializable {
    private int requestedQos;
    private String clientId;
    private String topic;
    private boolean cleanSession;
    private boolean active = true;
    private Date createTime;
    private String nodeBelongTo;

    public Subscription() {

    }

    public Subscription(String clientId, String topic, int requestedQos, boolean cleanSession) {
        this.requestedQos = requestedQos;
        this.clientId = clientId;
        this.topic = topic;
        this.cleanSession = cleanSession;
    }

    public String getClientId() {
        return clientId;
    }

    public int getRequestedQos() {
        return requestedQos;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isCleanSession() {
        return this.cleanSession;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public void setRequestedQos(int requestedQos) {
        this.requestedQos = requestedQos;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public String getNodeBelongTo() {
        return nodeBelongTo;
    }

    public void setNodeBelongTo(String nodeBelongTo) {
        this.nodeBelongTo = nodeBelongTo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Subscription other = (Subscription) obj;
        if (this.requestedQos != other.requestedQos) {
            return false;
        }
        if ((this.clientId == null) ? (other.clientId != null) : !this.clientId.equals(other.clientId)) {
            return false;
        }
        if ((this.topic == null) ? (other.topic != null) : !this.topic.equals(other.topic)) {
            return false;
        }
        return true;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + requestedQos;
        hash = 37 * hash + (this.clientId != null ? this.clientId.hashCode() : 0);
        hash = 37 * hash + (this.topic != null ? this.topic.hashCode() : 0);
        return hash;
    }

    /**
     * Trivial match method
     */
    boolean match(String topic) {
        return this.topic.equals(topic);
    }

    @Override
    public String toString() {
        return String.format("[t:%s, cliID: %s, qos: %s]", this.topic, this.clientId, this.requestedQos);
    }
}
