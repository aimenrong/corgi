package corgi.hub.core.mqtt.listener;

/**
 * Created by Terry LIANG on 2017/1/21.
 */
public interface TopicListenable {
    void registerListener(TopicListener topicListener);

    void unregisterListener(String listenerName);
}
