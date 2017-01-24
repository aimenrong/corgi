package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.listener.QueueListener;

/**
 * Created by Terry LIANG on 2017/1/20.
 */
public abstract class AbstractQueueListener implements QueueListener {
    protected String name;

    @Override
    public void setListenerName(String name) {
        this.name = name;
    }

    @Override
    public String getListenerName() {
        return this.name;
    }
}
