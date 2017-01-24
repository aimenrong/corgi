package corgi.hub.core.mqtt.bean;

import corgi.hub.core.mqtt.ServerChannel;

/**
 * Created by Terry LIANG on 2016/12/23.
 */
public class MqttSession implements HubSession {
    private String clientId;
    private ServerChannel channel;
    private boolean cleanSession;

    public MqttSession(String clientId, ServerChannel channel, boolean cleanSession) {
        this.clientId = clientId;
        this.channel = channel;
        this.cleanSession = cleanSession;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public String getClientId() {
        return clientId;
    }

    public ServerChannel getChannel() {
        return channel;
    }
}
