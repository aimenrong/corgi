package corgi.hub.core.mqtt.service;

import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.bean.RetainedMessage;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.common.IMatchingCondition;
import corgi.hub.core.mqtt.event.MqttEvent;
import corgi.hub.core.mqtt.event.MqttPublishEvent;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public interface IStorageService {
    void storeRetainedMessage(String topic, byte[] message, int qos);

    Collection<RetainedMessage> searchMatchingRetainedMessage(IMatchingCondition condition);

    ///////////////////////////////////////////////////////////////////////
    void addSubscription(Subscription newSubscription);

    void removeSubscriptionByClientId(String clientId);

    void removeSubscription(String clientId, String topic);

    List<Subscription> retrieveAllSubscriptions();

    List<Subscription> retrieveSubscriptionsByClientId(String clientId);

    List<Subscription> retrieveSubscriptionByTopic(String topic);

    Subscription retrieveSubscription(String clientId, String topic);

    void updateSubscription(Subscription subscription);

    ///////////////////////////////////////////////////////////////////////
    MqttStoreMessage addStoreMessage(MqttPublishEvent mqttPublishEvent);

    List<MqttStoreMessage> retrieveStoreMessage(String topic);

    MqttStoreMessage retrieveStoreMessage(long msgId);

    void removeStoreMessage(String clientId, int msgId, String topic);

    void removeStoreMessage(long storeMsgId);

    void ackStoreMessage(String clientId, long storeMsgId);

    boolean alreadyAck(String clientId, long storeMsgId);

    List<MqttStoreMessage> retrieveUnackStoreMessage(String clientId, String topic);

    List<MqttEvent> retrieveOutdatedPublishMessage(long lifecycle);

    ///////////////////////////////////////////////////////////////////////
    void storePublishedMessage(MqttEvent mqttEvent);

    void removePublishedMessage(String publishKey);

    void removePublishedMessageByClientId(String clientId);

    MqttEvent retrievePublishedMessage(String publishKey);

    List<MqttEvent> retrivePublishedMessageByClientId(String clientId);

    List<MqttEvent> retrievePublishedMessageByBatch(int batchSize);

    ///////////////////////////////////////////////////////////////////////
    void loggingNotifyEvent(long storeMsgId);

    void registerNodeInfo();

    int getOnlineNodeCount();

    void ackNotifyEvent(long storeMsgId);

    Set<Long> getUnackNotifyEventWithPeriodInSecond(int totalOnlineAcker, int period);

}
