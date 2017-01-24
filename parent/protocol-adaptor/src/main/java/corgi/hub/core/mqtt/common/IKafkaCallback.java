package corgi.hub.core.mqtt.common;

/**
 * Created by Terry LIANG on 2017/1/7.
 */
public interface IKafkaCallback<T> {
    void callback(HubContext context, T callbackObject);
}
