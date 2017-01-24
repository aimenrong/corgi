package corgi.hub.core.mqtt.listener;

/**
 * Created by Terry LIANG on 2017/1/21.
 */
public interface TopicListener {

    void onMessage(Object message);

    String getListenerName();
}
