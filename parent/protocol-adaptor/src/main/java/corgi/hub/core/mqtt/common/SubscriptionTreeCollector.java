package corgi.hub.core.mqtt.common;

import corgi.hub.core.mqtt.bean.Subscription;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public class SubscriptionTreeCollector  implements IVisitor<List<Subscription>> {

    private List<Subscription> m_allSubscriptions = new ArrayList<Subscription>();

    public void visit(TreeNode node) {
        m_allSubscriptions.addAll(node.subscriptions());
    }

    public List<Subscription> getResult() {
        return m_allSubscriptions;
    }
}
