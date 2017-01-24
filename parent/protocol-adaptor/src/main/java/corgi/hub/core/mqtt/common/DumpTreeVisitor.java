package corgi.hub.core.mqtt.common;

import corgi.hub.core.mqtt.bean.Subscription;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public class DumpTreeVisitor implements IVisitor<String> {
    String s = "";
    public void visit(TreeNode node) {
        String subScriptionsStr = "";
        for (Subscription sub : node.getSubscriptions()) {
            subScriptionsStr += sub.toString();
        }
        s += node.getToken() == null ? "" : node.getToken().toString();
        s += subScriptionsStr + "\n";
    }

    public String getResult() {
        return s;
    }
}
