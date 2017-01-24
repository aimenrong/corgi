package corgi.hub.core.mqtt.service;

import corgi.hub.core.mqtt.bean.Subscription;

/**
 * Created by Terry LIANG on 2017/1/17.
 */
public interface IQueueService {

    void listenQueue(Subscription subscription);

    void unlistenQueue(Subscription subscription);

    void pushQueueMessage(String queue, long storeMsgId);

}
