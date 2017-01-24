package corgi.hub.core.mqtt.common;

import corgi.hub.core.mqtt.bean.HubSession;

/**
 * Created by Terry LIANG on 2017/1/7.
 */
public interface HubContext {
    HubSession getSession(String clientId);

    boolean sessionExist(String clientId);

    void putSession(String clientId, HubSession session);

    HubSession remove(String clientId);

    ISubscriptionManager getSubscriptionManager();

    String getNodeName();
}
