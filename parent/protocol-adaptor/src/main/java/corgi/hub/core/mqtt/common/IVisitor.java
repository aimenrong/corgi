package corgi.hub.core.mqtt.common;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public interface IVisitor<T> {
    void visit(TreeNode node);

    T getResult();
}
