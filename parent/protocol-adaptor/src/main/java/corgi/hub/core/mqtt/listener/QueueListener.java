package corgi.hub.core.mqtt.listener;

import corgi.hub.core.mqtt.exception.NoConsumerAvailableException;

/**
 * Created by Terry LIANG on 2017/1/19.
 */
public interface QueueListener {
    void onQueueMessage(Object message) throws NoConsumerAvailableException;

    void setListenerName(String name);

    String getListenerName();
}
