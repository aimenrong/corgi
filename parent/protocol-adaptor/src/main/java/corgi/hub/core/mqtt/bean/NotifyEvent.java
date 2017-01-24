package corgi.hub.core.mqtt.bean;

/**
 * Created by Terry LIANG on 2017/1/15.
 */
public class NotifyEvent {
    private long storeMsgId;

    private long createTime;

    private int nodeNumberStart;

    private int nodeNumberEnd;

    public long getStoreMsgId() {
        return storeMsgId;
    }

    public void setStoreMsgId(long storeMsgId) {
        this.storeMsgId = storeMsgId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public int getNodeNumberStart() {
        return nodeNumberStart;
    }

    public void setNodeNumberStart(int nodeNumberStart) {
        this.nodeNumberStart = nodeNumberStart;
    }

    public int getNodeNumberEnd() {
        return nodeNumberEnd;
    }

    public void setNodeNumberEnd(int nodeNumberEnd) {
        this.nodeNumberEnd = nodeNumberEnd;
    }
}
