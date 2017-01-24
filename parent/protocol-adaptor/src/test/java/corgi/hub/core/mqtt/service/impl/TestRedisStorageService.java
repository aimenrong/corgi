package corgi.hub.core.mqtt.service.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.RetainedMessage;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.common.IMatchingCondition;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.event.MqttEvent;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.exception.RedisStorageException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestRedisStorageService {
    private RedisStorageService redisStorageService;
    @Mock
    private JedisConnectionManager jedisConnectionManager;
    @Mock
    private Jedis jedis;

    private static final String TABLE_PUBLISH_EVENTS = "publish_messages";
    private static final String TABLE_SUBSCRIPTIONS = "subscriptions";
    private static final String TABLE_RETAIN = "retain";
    private static final String SEPARATOR = ":";
    private String topic;
    private byte[] message;
    private int qos;
    @Mock
    private ScanResult<String> result;

    @Before
    public void init() {
        redisStorageService = new RedisStorageService();
        redisStorageService.setJedisConnectionManager(jedisConnectionManager);
        when(jedisConnectionManager.getPersistConnection()).thenReturn(jedis);
        topic = "topic1";
        message = "hello".getBytes();
    }

    @Test
    public void testStoreRetainedMessage() {
        // case 1
        String tableKey = TABLE_RETAIN + SEPARATOR + topic + SEPARATOR + "value";
        String tableValue = JSON.toJSONString(new RetainedMessage(message, qos, topic), false);
        redisStorageService.storeRetainedMessage(topic, message, qos);
        verify(jedis, times(1)).set(tableKey, tableValue);
        // case 2
        redisStorageService.storeRetainedMessage("topic1", "".getBytes(), qos);
        tableValue = JSON.toJSONString(new RetainedMessage("".getBytes(), qos, topic), false);
        verify(jedis, times(1)).del(tableKey);
        // case 3
        doThrow(new RuntimeException("test exception")).when(jedis).set(anyString(), anyString());
        try {
            redisStorageService.storeRetainedMessage(topic, message, 0);
        } catch (Exception e) {
            Assert.assertEquals(e.getClass(), RedisStorageException.class);
        }
        // case 4
        verify(jedisConnectionManager, times(3)).getPersistConnection();
        verify(jedis, times(3)).close();
    }

    @Test
    public void testSearchMatchingRetainedMessage() {
        // case 1
        when(jedis.scan(anyString(), any())).thenReturn(result);
        String jsonString = JSON.toJSONString(new RetainedMessage(message, 0, topic));
        List<String> result2 = Lists.newArrayList(jsonString);
        when(result.getResult()).thenReturn(result2);
        List<String> retainedMessages = Lists.newArrayList(jsonString);
        when(jedis.mget(anyString())).thenReturn(retainedMessages);
        IMatchingCondition condition = Mockito.mock(IMatchingCondition.class);
        when(condition.match(anyString())).thenReturn(true);
        Collection<RetainedMessage> list = redisStorageService.searchMatchingRetainedMessage(condition);
        Iterator<RetainedMessage> it = list.iterator();
        Assert.assertTrue(it.hasNext());
        RetainedMessage oStoreMessage = list.iterator().next();
        Assert.assertEquals(oStoreMessage.getTopic(), topic);
        Assert.assertEquals(new String(oStoreMessage.getPayload()), new String(message));
        Assert.assertEquals(oStoreMessage.getQos(), 0);
        // case 2
        doThrow(new RuntimeException()).when(jedis).scan(anyString());
        try {
            redisStorageService.searchMatchingRetainedMessage(condition);
        } catch (Exception e) {
            Assert.assertEquals(e.getClass(), RedisStorageException.class);
        }
        verify(jedis, times(2)).close();
    }

    @Test
    public void testStorePublishedMessage() {
        // case 1
        MqttPublishEvent mqttPublishEvent = new MqttPublishEvent();
        mqttPublishEvent.setClientId("clientId");
        mqttPublishEvent.setMsgId(1);
        redisStorageService.storePublishedMessage(mqttPublishEvent);
        verify(jedis, times(1)).set(anyString(), anyString());
        // case 2
        doThrow(new RuntimeException("test exception")).when(jedis).set(anyString(), anyString());
        try {
            redisStorageService.storePublishedMessage(mqttPublishEvent);
        } catch (Exception e) {
            Assert.assertEquals(e.getClass(), RedisStorageException.class);
        }
    }

    @Test
    public void testRetrivePublishedMessageByClientId() {
        // case 1
        ServerChannel channel = Mockito.mock(ServerChannel.class);
        MqttPublishEvent mqttPublishEvent = new MqttPublishEvent(topic, 0, message, false, "clientId", channel);
        when(jedis.scan(anyString(), any())).thenReturn(result);
        String jsonString = JSON.toJSONString(mqttPublishEvent);
        List<String> result2 = Lists.newArrayList(jsonString);
        when(result.getResult()).thenReturn(result2);
        List<String> retainedMessages = Lists.newArrayList(jsonString);
        when(jedis.mget(anyString())).thenReturn(retainedMessages);
        IMatchingCondition condition = Mockito.mock(IMatchingCondition.class);
        when(condition.match(anyString())).thenReturn(true);
        List<MqttEvent> list = redisStorageService.retrivePublishedMessageByClientId("clientId");
        Iterator<MqttEvent> it = list.iterator();
        Assert.assertTrue(it.hasNext());
        MqttPublishEvent oMqttPublishEvent = (MqttPublishEvent) list.iterator().next();
        Assert.assertEquals(oMqttPublishEvent.getTopic(), topic);
        Assert.assertEquals(new String(oMqttPublishEvent.getMessage()), new String(message));
        Assert.assertEquals(oMqttPublishEvent.getQos(), 0);
        Assert.assertFalse(oMqttPublishEvent.isRetain());
        Assert.assertEquals(oMqttPublishEvent.getClientId(), "clientId");
        // case 2
    }

    @Test
    public void testRetrievePublishedMessage() {
        ServerChannel channel = Mockito.mock(ServerChannel.class);
        MqttPublishEvent mqttPublishEvent = new MqttPublishEvent(topic, 0, message, false, "clientId", channel);
        when(jedis.get(anyString())).thenReturn(JSON.toJSONString(mqttPublishEvent));
        MqttPublishEvent oEvent = (MqttPublishEvent) redisStorageService.retrievePublishedMessage("clientId0");
        Assert.assertEquals(oEvent.getTopic(), topic);
        Assert.assertEquals(new String(oEvent.getMessage()), new String(message));
        Assert.assertEquals(oEvent.getQos(), 0);
        Assert.assertFalse(oEvent.isRetain());
        Assert.assertEquals(oEvent.getClientId(), "clientId");
    }

    @Test
    public void testRemovePublishedMessage() {
        redisStorageService.removePublishedMessage("");
        verify(jedis, times(1)).del(anyString());
    }

    @Test
    public void testRemovePublishedMessageByClientId() {
        when(jedis.scan(anyString(), any())).thenReturn(result);
        List<String> result2 = Lists.newArrayList("abc");
        when(result.getResult()).thenReturn(result2);
        List<String> retainedMessages = Lists.newArrayList("abc");
        redisStorageService.removePublishedMessageByClientId("");
        verify(jedis, times(1)).del(anyString());
    }

    @Test
    public void testAddSubscription() {
        Subscription subscription = new Subscription();
        subscription.setActive(true);
        subscription.setCleanSession(false);
        subscription.setClientId("clientId1");
        subscription.setRequestedQos(1);
        subscription.setTopic("topic1");
        String clientId = subscription.getClientId();
        String topic = subscription.getTopic();
        String setKey = TABLE_SUBSCRIPTIONS + SEPARATOR + clientId + SEPARATOR + "topic";
        String setValue = topic;
        String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + topic + SEPARATOR + "value";
        String tableValue = JSON.toJSONString(subscription, false);
        redisStorageService.addSubscription(subscription);
        Mockito.verify(jedis, times(1)).sadd(setKey, setValue);
        Mockito.verify(jedis, times(1)).set(tableKey, tableValue);
    }

    @Test
    public void testRetrieveAllSubscriptions() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        when(jedis.scan(anyString(), any())).thenReturn(result);
        String jsonString = JSON.toJSONString(subscription);
        List<String> result2 = Lists.newArrayList(jsonString);
        when(result.getResult()).thenReturn(result2);
        List<String> subscriptions = Lists.newArrayList(jsonString);
        when(jedis.mget(anyString())).thenReturn(subscriptions);
        List<Subscription> list = redisStorageService.retrieveAllSubscriptions();
        Iterator<Subscription> it = list.iterator();
        Assert.assertTrue(it.hasNext());
        Subscription oSubscription = it.next();
        Assert.assertEquals(oSubscription.getClientId(), "clientId");
        Assert.assertEquals(oSubscription.getTopic(), topic);
        Assert.assertEquals(oSubscription.getRequestedQos(), 0);
        Assert.assertTrue(oSubscription.isCleanSession());
    }

    @Test
    public void testRetrieveSubscriptionsByClientId() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        when(jedis.scan(anyString(), any())).thenReturn(result);
        String jsonString = JSON.toJSONString(subscription);
        List<String> result2 = Lists.newArrayList(jsonString);
        when(result.getResult()).thenReturn(result2);
        List<String> subscriptions = Lists.newArrayList(jsonString);
        when(jedis.mget(anyString())).thenReturn(subscriptions);
        List<Subscription> list = redisStorageService.retrieveSubscriptionsByClientId("clientId");
        Iterator<Subscription> it = list.iterator();
        Assert.assertTrue(it.hasNext());
        Subscription oSubscription = it.next();
        Assert.assertEquals(oSubscription.getClientId(), "clientId");
        Assert.assertEquals(oSubscription.getTopic(), topic);
        Assert.assertEquals(oSubscription.getRequestedQos(), 0);
        Assert.assertTrue(oSubscription.isCleanSession());
    }

    @Test
    public void testRemoveSubscription() {
        redisStorageService.removeSubscription("clientId", topic);
        verify(jedis, times(1)).del(anyString());
    }

    @Test
    public void testRemoveSubscriptionByClientId() {
        Subscription subscription = new Subscription("clientId", topic, 0, true);
        when(jedis.scan(anyString(), any())).thenReturn(result);
        String jsonString = JSON.toJSONString(subscription);
        List<String> result2 = Lists.newArrayList(jsonString);
        when(result.getResult()).thenReturn(result2);
        List<String> subscriptions = Lists.newArrayList(jsonString);
        when(jedis.mget(anyString())).thenReturn(subscriptions);
        redisStorageService.removeSubscriptionByClientId("abc");
        verify(jedis, times(1)).del(anyString());
    }
}
