package corgi.hub.core.mqtt.common;

import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.service.IStorageService;

import java.util.List;

/**
 * Created by Terry LIANG on 2017/1/7.
 */
public interface ISubscriptionManager {
    IStorageService getStorageService();

    Subscription removeSubscription(String topic, String clientID);

    void add(Subscription newSubscription);

    void clearAllSubscriptions();

    void removeForClient(String clientId);

    void cleanupOfflineSubscriptino(String clientId);

    void deactivate(String clientID);

    void activate(String clientID);

    List<Subscription> matches(String topic);

    boolean contains(Subscription sub);

    int size();

    String dumpTree();

    void activateSubscription(String clientId);

    void deactivateSubscription(String clientId);
}
