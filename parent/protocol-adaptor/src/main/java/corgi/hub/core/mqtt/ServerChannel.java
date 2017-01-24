package corgi.hub.core.mqtt;

/**
 * Created by Terry LIANG on 2016/12/23.
 */
public interface ServerChannel {
    Object getAttribute(String key);

    void setAttribute(String key, Object value);

    void setIdleTime(int idleTime);

    void close(boolean immediately);

    void write(Object value);

    boolean isActive();
}
