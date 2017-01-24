package corgi.hub.core.mqtt.common;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public interface IMatchingCondition {
    boolean match(String key);
}
