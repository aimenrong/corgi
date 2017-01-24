package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.listener.QueueListenable;
import corgi.hub.core.mqtt.listener.QueueListener;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Terry LIANG on 2017/1/20.
 */
public class GeneralQueueListenable implements QueueListenable {
    protected String name;
    protected Map<String, QueueListener> queueListenerMap = new HashMap<>();

    @Override
    public void registerListener(QueueListener queueListener) {
        queueListenerMap.put(queueListener.getListenerName(), queueListener);
    }

    @Override
    public void unregisterListener(String name) {
        queueListenerMap.remove(name);
    }

    @Override
    public void setListenableName(String name) {
        this.name = name;
    }

    @Override
    public String getListenableName() {
        return this.name;
    }
}
