package corgi.hub.core.mqtt.common;

import corgi.hub.core.mqtt.bean.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
public class TreeNode {
    private TreeNode parent;
    private Token token;
    private List<TreeNode> children = new ArrayList<TreeNode>();
    private List<Subscription> subscriptions = new ArrayList<Subscription>();

    public TreeNode(TreeNode parent) {
        this.parent = parent;
    }

    Token getToken() {
        return token;
    }

    public void setToken(Token topic) {
        this.token = topic;
    }

    public void addSubscription(Subscription s) {
        //avoid double registering
        if (subscriptions.contains(s)) {
            return;
        }
        subscriptions.add(s);
    }

    public void addChild(TreeNode child) {
        children.add(child);
    }

    public boolean isLeaf() {
        return children.isEmpty();
    }

    /**
     * Search for children that has the specified token, if not found return
     * null;
     */
    public TreeNode childWithToken(Token token) {
        for (TreeNode child : children) {
            if (child.getToken().equals(token)) {
                return child;
            }
        }

        return null;
    }

    public List<Subscription> subscriptions() {
        return subscriptions;
    }

    public  void matches(Queue<Token> tokens, List<Subscription> matchingSubs) {
        Token t = tokens.poll();

        //check if t is null <=> tokens finished
        if (t == null) {
            matchingSubs.addAll(subscriptions);
            //check if it has got a MULTI child and add its subscriptions
            for (TreeNode n : children) {
                if (n.getToken() == Token.MULTI) {
                    matchingSubs.addAll(n.subscriptions());
                }
            }

            return;
        }

        //we are on MULTI, than add subscriptions and return
        if (token == Token.MULTI) {
            matchingSubs.addAll(subscriptions);
            return;
        }

        for (TreeNode n : children) {
            if (n.getToken().match(t)) {
                //Create a copy of token, else if navigate 2 sibling it
                //consumes 2 elements on the queue instead of one
                n.matches(new LinkedBlockingQueue<Token>(tokens), matchingSubs);
            }
        }
    }

    /**
     * Return the number of registered subscriptions
     */
    public int size() {
        int res = subscriptions.size();
        for (TreeNode child : children) {
            res += child.size();
        }
        return res;
    }

    public void removeClientSubscriptions(String clientID) {
        //collect what to delete and then delete to avoid ConcurrentModification
        List<Subscription> subsToRemove = new ArrayList<Subscription>();
        for (Subscription s : subscriptions) {
            if (s.getClientId().equals(clientID)) {
                subsToRemove.add(s);
            }
        }

        for (Subscription s : subsToRemove) {
            subscriptions.remove(s);
        }

        //go deep
        for (TreeNode child : children) {
            child.removeClientSubscriptions(clientID);
        }
    }

    /**
     * Deactivate all topic subscriptions for the given clientID.
     * */
    public void deactivate(String clientID) {
        for (Subscription s : subscriptions) {
            if (s.getClientId().equals(clientID)) {
                s.setActive(false);
            }
        }

        //go deep
        for (TreeNode child : children) {
            child.deactivate(clientID);
        }
    }

    /**
     * Activate all topic subscriptions for the given clientID.
     * */
    public void activate(String clientId) {
        for (Subscription s : subscriptions) {
            if (s.getClientId().equals(clientId)) {
                s.setActive(true);
            }
        }
        //go deep
        for (TreeNode child : children) {
            child.activate(clientId);
        }
    }

    public TreeNode getParent() {
        return parent;
    }

    public void setParent(TreeNode parent) {
        this.parent = parent;
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    public List<Subscription> getSubscriptions() {
        return subscriptions;
    }
}
