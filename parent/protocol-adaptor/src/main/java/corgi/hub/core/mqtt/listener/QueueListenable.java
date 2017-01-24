package corgi.hub.core.mqtt.listener;

/**
 * Created by Terry LIANG on 2017/1/19.
 */
public interface QueueListenable {
    void registerListener(QueueListener queueListener);

    void unregisterListener(String name);

    void setListenableName(String name);

    String getListenableName();
}
