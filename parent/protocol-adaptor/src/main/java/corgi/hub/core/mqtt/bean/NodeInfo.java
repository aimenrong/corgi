package corgi.hub.core.mqtt.bean;

/**
 * Created by Terry LIANG on 2017/1/15.
 */
public class NodeInfo {

    private int nodeId;

    private String nodeName;

    private String nodeIp;

    public NodeInfo(String nodeName, String ip) {
        this.nodeName = nodeName;
        this.nodeIp = ip;
    }

    public NodeInfo() {

    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }
}
