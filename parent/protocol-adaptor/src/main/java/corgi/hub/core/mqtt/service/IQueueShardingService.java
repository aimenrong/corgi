package corgi.hub.core.mqtt.service;

import corgi.hub.core.mqtt.bean.Subscription;

/**
 * Created by Terry LIANG on 2017/1/17.
 */
public interface IQueueShardingService {

    void putShard(String queue, Subscription subscription);

    Subscription removeShard(String queue, Subscription subscription);

    int size(String queue);
}
