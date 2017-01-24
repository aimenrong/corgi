package corgi.hub.core.mqtt.exception;

/**
 * Created by Terry LIANG on 2017/1/7.
 */
public class RedisStorageException extends Exception {
    public RedisStorageException(String message, Throwable t) {
        super(message, t);
    }

    public RedisStorageException(String message) {
        super(message);
    }

    public RedisStorageException(Throwable t) {
        super(t);
    }
}
