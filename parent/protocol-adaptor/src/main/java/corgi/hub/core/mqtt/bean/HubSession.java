package corgi.hub.core.mqtt.bean;

import corgi.hub.core.mqtt.ServerChannel;

/**
 * Created by Terry LIANG on 2016/12/23.
 */
public interface HubSession {

    public boolean isCleanSession();

    public String getClientId();

    public ServerChannel getChannel();
}
