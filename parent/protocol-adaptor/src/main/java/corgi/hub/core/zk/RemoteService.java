package corgi.hub.core.zk;

import corgi.hub.core.mqtt.event.MqttPublishEvent;

/**
 * Created by Terry LIANG on 2016/12/30.
 */
public interface RemoteService {
    void publishToRemoteSubscriber(MqttPublishEvent event);


}
