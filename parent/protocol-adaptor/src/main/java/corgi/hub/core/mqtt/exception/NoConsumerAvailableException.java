package corgi.hub.core.mqtt.exception;

/**
 * Created by Terry LIANG on 2017/1/21.
 */
public class NoConsumerAvailableException extends Exception {
    private String topicToBelong;

    public NoConsumerAvailableException(String message, Throwable t) {
        super(message, t);
    }

    public NoConsumerAvailableException(String message) {
        super(message);
    }

    public NoConsumerAvailableException(Throwable t) {
        super(t);
    }

    public String getTopicToBelong() {
        return topicToBelong;
    }

    public void setTopicToBelong(String topicToBelong) {
        this.topicToBelong = topicToBelong;
    }
}
