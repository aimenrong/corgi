package corgi.hub.core.mqtt.event;

import com.lmax.disruptor.EventFactory;

/**
 * Created by Terry LIANG on 2016/12/23.
 */
public class CoreEvent<T> {
    private T event;

    public T getEvent() {
        return event;
    }

    public void setEvent(T event) {
        this.event = event;
    }

    public final static EventFactory<CoreEvent> EVENT_FACTORY = new EventFactory<CoreEvent>() {

        public CoreEvent newInstance() {
            return new CoreEvent();
        }
    };
}
