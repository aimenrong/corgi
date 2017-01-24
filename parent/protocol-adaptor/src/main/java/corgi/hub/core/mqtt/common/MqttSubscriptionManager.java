package corgi.hub.core.mqtt.common;

import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.service.IStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by Terry LIANG on 2016/12/23.
 */
@Component
public class MqttSubscriptionManager implements ISubscriptionManager {
    private Logger logger = LoggerFactory.getLogger(MqttSubscriptionManager.class);

    private TreeNode subscriptions = new TreeNode(null);

    @Resource(name = "redisStorageService")
    private IStorageService storageService;

    public MqttSubscriptionManager() {
    }

    public TreeNode getSubscriptions() {
        return subscriptions;
    }

    /**
     * Initialize basic store structures, like the FS storage to maintain
     * client's topics subscriptions
     */
    @PostConstruct
    public void init() {
        //reload any subscriptions persisted
        for (Subscription subscription : storageService.retrieveAllSubscriptions()) {
            if (subscription.isActive()) {
                addDirect(subscription);
            }
        }
    }

    private void addDirect(Subscription newSubscription) {
        if (null != newSubscription) {
            TreeNode current = findMatchingNode(newSubscription.getTopic());
            current.addSubscription(newSubscription);
        }
    }

    private TreeNode findMatchingNode(String topic) {
        List<Token> tokens = new ArrayList<Token>();
        try {
            tokens = splitTopic(topic);
        } catch (ParseException ex) {
//            return;
        }

        TreeNode current = subscriptions;
        for (Token token : tokens) {
            TreeNode matchingChildren;

            //check if a children with the same token already exists
            if ((matchingChildren = current.childWithToken(token)) != null) {
                current = matchingChildren;
            } else {
                //create a new node for the newly inserted token
                matchingChildren = new TreeNode(current);
                matchingChildren.setToken(token);
                current.addChild(matchingChildren);
                current = matchingChildren;
            }
        }
        return current;
    }

    @Override
    public Subscription removeSubscription(String topic, String clientId) {
        TreeNode matchNode = findMatchingNode(topic);

        //search fr the subscription to remove
        Subscription toBeRemoved = null;
        for (Subscription sub : matchNode.subscriptions()) {
            if (sub.getTopic().equals(topic)) {
                toBeRemoved = sub;
                break;
            }
        }

        if (toBeRemoved != null) {
            matchNode.subscriptions().remove(toBeRemoved);
        }
        return toBeRemoved;
    }

    @Override
    public void add(Subscription newSubscription) {
        addDirect(newSubscription);
        if (newSubscription.getRequestedQos() != 0) {
            storageService.addSubscription(newSubscription);
        }
    }

    /**
     * TODO implement testing
     */
    @Override
    public void clearAllSubscriptions() {
        SubscriptionTreeCollector subsCollector = new SubscriptionTreeCollector();
        bfsVisit(subscriptions, subsCollector);

        List<Subscription> allSubscriptions = subsCollector.getResult();
        for (Subscription subscription : allSubscriptions) {
            removeSubscription(subscription.getTopic(), subscription.getClientId());
        }
    }

    /**
     * Visit the topics tree to remove matching subscriptions with clientID
     */
    @Override
    public void removeForClient(String clientId) {
        // remove from cache
        subscriptions.removeClientSubscriptions(clientId);
        // remove from storage
        storageService.removeSubscriptionByClientId(clientId);
    }

    public void cleanupOfflineSubscriptino(String clientId) {
        // remove from cache
        subscriptions.removeClientSubscriptions(clientId);
        List<Subscription> subscriptionList = storageService.retrieveSubscriptionsByClientId(clientId);
        for(Subscription subscription : subscriptionList) {
            subscription.setActive(false);
            storageService.updateSubscription(subscription);
        }
    }

    @Override
    public void deactivate(String clientId) {
        subscriptions.deactivate(clientId);
    }

    @Override
    public void activate(String clientId) {
        subscriptions.activate(clientId);
    }

    /**
     * Given a topic string return the clients subscriptions that matches it.
     * Topic string can't contain character # and + because they are reserved to
     * listeners subscriptions, and not topic publishing.
     */
    @Override
    public List<Subscription> matches(String topic) {
        List<Token> tokens = new ArrayList<Token>();
        try {
            tokens = splitTopic(topic);
        } catch (ParseException ex) {
        }

        Queue<Token> tokenQueue = new LinkedBlockingDeque<Token>(tokens);
        List<Subscription> matchingSubs = new ArrayList<Subscription>();
        subscriptions.matches(tokenQueue, matchingSubs);
        return matchingSubs;
    }

    @Override
    public boolean contains(Subscription sub) {
        return !matches(sub.getTopic()).isEmpty();
    }

    @Override
    public int size() {
        return subscriptions.size();
    }

    @Override
    public String dumpTree() {
        DumpTreeVisitor visitor = new DumpTreeVisitor();
        bfsVisit(subscriptions, visitor);
        return visitor.getResult();
    }

    private void bfsVisit(TreeNode node, IVisitor visitor) {
        if (node == null) {
            return;
        }
        visitor.visit(node);
        for (TreeNode child : node.getChildren()) {
            bfsVisit(child, visitor);
        }
    }

    /**
     * Verify if the 2 topics matching respecting the rules of MQTT Appendix A
     */
    public static boolean matchTopics(String msgTopic, String subscriptionTopic) {
        try {
            List<Token> msgTokens = MqttSubscriptionManager.splitTopic(msgTopic);
            List<Token> subscriptionTokens = MqttSubscriptionManager.splitTopic(subscriptionTopic);
            int i = 0;
            for (; i < subscriptionTokens.size(); i++) {
                Token subToken = subscriptionTokens.get(i);
                if (subToken != Token.MULTI && subToken != Token.SINGLE) {
                    Token msgToken = msgTokens.get(i);
                    if (!msgToken.equals(subToken)) {
                        return false;
                    }
                } else {
                    if (subToken == Token.MULTI) {
                        return true;
                    }
                    if (subToken == Token.SINGLE) {
                        //skip a step forward
                    }
                }
            }
            return i == msgTokens.size();
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static List<Token> splitTopic(String topic) throws ParseException {
        List res = new ArrayList<Token>();
        String[] splitted = topic.split("/");
        if (splitted.length == 0) {
            res.add(Token.EMPTY);
        }
        for (int i = 0; i < splitted.length; i++) {
            String s = splitted[i];
            if (s.isEmpty()) {
                if (i != 0) {
                    throw new ParseException("Bad format of topic, expetec topic name between separators", i);
                }
                res.add(Token.EMPTY);
            } else if (s.equals("#")) {
                //check that multi is the last symbol
                if (i != splitted.length - 1) {
                    throw new ParseException("Bad format of topic, the multi symbol (#) has to be the last one after a separator", i);
                }
                res.add(Token.MULTI);
            } else if (s.contains("#")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else if (s.equals("+")) {
                res.add(Token.SINGLE);
            } else if (s.contains("+")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else {
                res.add(new Token(s));
            }
        }

        return res;
    }

    @Override
    public void activateSubscription(String clientId) {
        subscriptions.activate(clientId);
    }

    @Override
    public void deactivateSubscription(String clientId) {
        subscriptions.deactivate(clientId);
    }

    @Override
    public IStorageService getStorageService() {
        return this.storageService;
    }

    public void setStorageService(IStorageService storageService) {
        this.storageService = storageService;
    }
}
