package corgi.hub.core.mqtt.event;

import corgi.hub.core.mqtt.ServerChannel;
import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * Created by Terry LIANG on 2016/12/27.
 */
public abstract class BaseEvent {
    protected String clientId;

    protected int keepAlived;

    protected boolean cleanSession;

    protected transient MqttMessage mqttMessage;

    protected transient ServerChannel channel;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public ServerChannel getChannel() {
        return channel;
    }

    public void setChannel(ServerChannel channel) {
        this.channel = channel;
    }

    public int getKeepAlived() {
        return keepAlived;
    }

    public void setKeepAlived(int keepAlived) {
        this.keepAlived = keepAlived;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public MqttMessage getMqttMessage() {
        return mqttMessage;
    }

    public void setMqttMessage(MqttMessage mqttMessage) {
        this.mqttMessage = mqttMessage;
    }
}
