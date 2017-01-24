package corgi.hub.core.mqtt.common;

import com.google.common.collect.Lists;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.service.impl.RedisStorageService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Created by Terry LIANG on 2017/1/8.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestMqttSubscriptionManager {
    private MqttSubscriptionManager mqttSubscriptionManager;
    @Mock
    private RedisStorageService storageService;
    private String topic = "topic1/+";
    private String clientId = "clientId";

    @Before
    public void init() {
        mqttSubscriptionManager = new MqttSubscriptionManager();
        mqttSubscriptionManager.setStorageService(storageService);
//        when(storageService.retrieveAllSubscriptions()).thenReturn(Lists.newArrayList(new Subscription(topic, clientId, 0, true)));
    }

    @Test
    public void testRemoveSubscription() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        Subscription subscription2 = new Subscription("clientId", "topic1", 0, true);
        when(storageService.retrieveAllSubscriptions()).thenReturn(Lists.newArrayList(subscription, subscription2));
        mqttSubscriptionManager.init();
        Subscription oSubscription = mqttSubscriptionManager.removeSubscription("topic1", "clientId");
        Assert.assertEquals(oSubscription.getClientId(), "clientId");
        Assert.assertEquals(oSubscription.getTopic(), "topic1");
    }

    @Test
    public void testAdd() {
        when(storageService.retrieveAllSubscriptions()).thenReturn(Lists.newArrayList());
        mqttSubscriptionManager.init();
        Subscription subscription2 = new Subscription("clientId", "topic1", 0, true);
        mqttSubscriptionManager.add(subscription2);
        Subscription oSubscription = mqttSubscriptionManager.removeSubscription("topic1", "clientId");
        Assert.assertEquals(oSubscription.getClientId(), "clientId");
        Assert.assertEquals(oSubscription.getTopic(), "topic1");
    }

    @Test
    public void testClearAllSubscriptions() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        Subscription subscription2 = new Subscription("clientId", "topic1", 0, true);
        when(storageService.retrieveAllSubscriptions()).thenReturn(Lists.newArrayList(subscription, subscription2));
        mqttSubscriptionManager.init();
        mqttSubscriptionManager.clearAllSubscriptions();
        Subscription oSubscription = mqttSubscriptionManager.removeSubscription("topic1", "clientId");
        Assert.assertNull(oSubscription);
    }

    @Test
    public void testRemoveForClient() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        Subscription subscription2 = new Subscription("clientId", "topic1", 0, true);
        Subscription subscription3 = new Subscription("clientId2", "topic1", 0, true);
        when(storageService.retrieveAllSubscriptions()).thenReturn(Lists.newArrayList(subscription, subscription2, subscription3));
        mqttSubscriptionManager.init();
        ////
        mqttSubscriptionManager.removeForClient("clientId");
        Subscription oSubscription = mqttSubscriptionManager.removeSubscription("topic1", "clientId2");
        Assert.assertEquals(oSubscription.getClientId(), "clientId2");
        Assert.assertEquals(oSubscription.getTopic(), "topic1");
        oSubscription = mqttSubscriptionManager.removeSubscription("topic1", "clientId");
        Assert.assertNull(oSubscription);
    }

    @Test
    public void testDeactivateAndActivate() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        Subscription subscription2 = new Subscription("clientId", "topic1", 0, true);
        Subscription subscription3 = new Subscription("clientId2", "topic1", 0, true);
        when(storageService.retrieveAllSubscriptions()).thenReturn(Lists.newArrayList(subscription, subscription2, subscription3));
        mqttSubscriptionManager.init();
        mqttSubscriptionManager.deactivate("clientId2");
        TreeNode treeNode = mqttSubscriptionManager.getSubscriptions();
        TreeNode matchNode = findMatchingNode("topic1", treeNode);
        boolean foundMatch = false;
        for (Subscription sub : matchNode.getSubscriptions()) {
            if (sub.getClientId().equals("clientId2")) {
                Assert.assertFalse(sub.isActive());
                foundMatch = true;
            } else {
                Assert.assertTrue(sub.isActive());
            }
        }
        Assert.assertTrue(foundMatch);
        ////
        mqttSubscriptionManager.activate("clientId2");
        foundMatch = false;
        for (Subscription sub : matchNode.getSubscriptions()) {
            if (sub.getClientId().equals("clientId2")) {
                Assert.assertTrue(sub.isActive());
                foundMatch = true;
            }
        }
        Assert.assertTrue(foundMatch);
    }

    private TreeNode findMatchingNode(String topic, TreeNode subscriptions) {
        List<Token> tokens = new ArrayList<Token>();
        try {
            tokens = MqttSubscriptionManager.splitTopic(topic);
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

    @Test
    public void testMatchesAndContains() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        Subscription subscription2 = new Subscription("clientId", "topic1", 0, true);
        Subscription subscription3 = new Subscription("clientId2", "topic1", 0, true);
        when(storageService.retrieveAllSubscriptions()).thenReturn(Lists.newArrayList(subscription, subscription2, subscription3));
        mqttSubscriptionManager.init();
        List<Subscription> list = mqttSubscriptionManager.matches("topic1");
        Assert.assertEquals(list.size(), 2);
        Subscription outOfSub = new Subscription("superMan", "topic8", 0, true);
        Assert.assertTrue(mqttSubscriptionManager.contains(subscription2));
        Assert.assertFalse(mqttSubscriptionManager.contains(outOfSub));
    }

    @Test
    public void testDumpTree() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        Subscription subscription2 = new Subscription("clientId", "topic1", 0, true);
        Subscription subscription3 = new Subscription("clientId2", "topic1", 0, true);
        when(storageService.retrieveAllSubscriptions()).thenReturn(Lists.newArrayList(subscription, subscription2, subscription3));
        mqttSubscriptionManager.init();
        String expected = "\ntopic1[t:topic1, cliID: clientId, qos: 0][t:topic1, cliID: clientId2, qos: 0]\n" +
                "+[t:topic1/+, cliID: clientId, qos: 0]\n";
        System.out.println(mqttSubscriptionManager.dumpTree());
        Assert.assertEquals(expected, mqttSubscriptionManager.dumpTree());
    }
}
