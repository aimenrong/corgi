package corgi.hub.core.mqtt.common;

import corgi.hub.core.mqtt.bean.HubSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Terry LIANG on 2017/1/7.
 */
@Component("mqttContext")
public class MqttContext implements HubContext {
    private Map<String, HubSession> clientSession = new HashMap<String, HubSession>();
    private static final String NODE_NAME_PREFIX = "protocol-adaptor-node-";
    @Value("${app.node.id}")
    private int nodeId;
    private String nodeName;

    @Autowired
    private MqttSubscriptionManager subscriptionManager;

    @Override
    public HubSession getSession(String clientId) {
        return clientSession.get(clientId);
    }

    @Override
    public boolean sessionExist(String clientId) {
        return clientSession.containsKey(clientId);
    }

    @Override
    public HubSession remove(String clientId) {
        return clientSession.remove(clientId);
    }

    @Override
    public ISubscriptionManager getSubscriptionManager() {
        return subscriptionManager;
    }

    @Override
    public void putSession(String clientId, HubSession session) {
        clientSession.put(clientId, session);
    }

    /**
     * Postpone the implementation due to involved zookeeper
     * @return
     */
    @Override
    public String getNodeName() {
        if (null == nodeName) {
            Random random = new Random();
            nodeName = NODE_NAME_PREFIX + nodeId;
        }
        return nodeName;
    }
}
