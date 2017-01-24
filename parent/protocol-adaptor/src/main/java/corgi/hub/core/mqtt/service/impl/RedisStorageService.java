package corgi.hub.core.mqtt.service.impl;

import com.alibaba.fastjson.JSON;
import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.common.IMatchingCondition;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.dao.RedisConstants;
import corgi.hub.core.mqtt.exception.RedisStorageException;
import corgi.hub.core.mqtt.bean.RetainedMessage;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.util.MqttUtil;
import corgi.hub.core.mqtt.event.MqttEvent;
import corgi.hub.core.mqtt.service.IStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.sortedset.ZAddParams;

import javax.annotation.Resource;
import java.util.*;

/**
 * Created by Terry LIANG on 2016/12/24.
 */
@Service("redisStorageService")
public class RedisStorageService  implements IStorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisStorageService.class);
    @Autowired
    private JedisConnectionManager jedisConnectionManager;

    private static final String TABLE_PUBLISH_EVENTS = RedisConstants.TABLE_PUBLISH_MESSAGE;
    private static final String TABLE_SUBSCRIPTIONS = RedisConstants.TABLE_SUBSCRIPTIONS;
    private static final String TABLE_RETAIN = RedisConstants.TABLE_RETAIN;
    private static final String TABLE_STORE_MESSAGE = RedisConstants.TABLE_STORE_MESSAGE;
    private static final String SEPARATOR = ":";
    @Resource(name = "storeMessageIdGenerator")
    private RedisAtomicLong storeMessageIdGenerator;

    public RedisStorageService() {
    }

    /**************************************** Retained Message *************************************************/
    @Override
    public void storeRetainedMessage(String topic, byte[] message, int qos) {
        String tableKey = TABLE_RETAIN + SEPARATOR + topic + SEPARATOR + "value";
        try {
            if (message.length == 0) {
                del(tableKey);
            } else {
                String tableValue = JSON.toJSONString(new RetainedMessage(message, qos, topic), false);
                save(tableKey, tableValue);
            }
        } catch (RedisStorageException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public Collection<RetainedMessage> searchMatchingRetainedMessage(IMatchingCondition condition) {
        List<RetainedMessage> results = new ArrayList<RetainedMessage>();
        for (RetainedMessage storedMsg : retrieveAllRetainedMessage()) {
            if (condition.match(storedMsg.getTopic())) {
                results.add(storedMsg);
            }
        }
        return results;
    }

    private Collection<RetainedMessage> retrieveAllRetainedMessage() {
        List<RetainedMessage> results = new ArrayList<RetainedMessage>();
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            ScanParams scanParams = new ScanParams();
            String tableKey = TABLE_RETAIN + SEPARATOR + "*";
            scanParams.match(tableKey);
            scanParams.count(100);
            ScanResult<String> result1 = jedis.scan("0", scanParams);
            List<String> result2 = result1.getResult();
            if (result2.size() > 0) {
                List<String> retainedMessages =  jedis.mget(result1.getResult().toArray(new String[0]));
                for (String result : retainedMessages) {
                    if (result != null) {
                        RetainedMessage msg = (RetainedMessage) JSON.parseObject(result, RetainedMessage.class);
                        results.add(msg);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        return results;
    }

    /**************************************** Published Message ********************************************/
    private static String INDEX_PUBLISH_MESSAGE_CREATETIME = "indexPublishMessageCreateTime";

    @Override
    public void storePublishedMessage(MqttEvent mqttEvent) {
        MqttPublishEvent mqttPublishEvent = (MqttPublishEvent) mqttEvent;
        String tableKey = TABLE_PUBLISH_EVENTS + SEPARATOR + MqttUtil.formatPublishKey(mqttPublishEvent.getClientId(), mqttPublishEvent.getMsgId()) + SEPARATOR + "value";
        String tablevalue = JSON.toJSONString(mqttPublishEvent);
        Jedis jedis = null;
        try {
            save(tableKey, tablevalue);
            // Add index for createTime
            jedis = jedisConnectionManager.getPersistConnection();
            long createTime = mqttPublishEvent.getCreateTime();
            String publishKey = MqttUtil.formatPublishKey(mqttPublishEvent.getClientId(), mqttPublishEvent.getMsgId());
            jedis.zadd(INDEX_PUBLISH_MESSAGE_CREATETIME, createTime, publishKey);
        } catch (RedisStorageException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    @Override
    public List<MqttEvent> retrivePublishedMessageByClientId(String clientId) {
        List<MqttEvent> allPublishEvents = new ArrayList<MqttEvent>();
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String tableKey = clientId + "*";
            String subKey = TABLE_PUBLISH_EVENTS + SEPARATOR + tableKey + SEPARATOR + "value";
            ScanParams scanParams = new ScanParams();
            scanParams.match(subKey);
            scanParams.count(100);
            ScanResult<String> result = jedis.scan("0", scanParams);
            List<String> pEventKeyList = result.getResult();
            if (pEventKeyList.size() > 0) {
                List<String> pEventList = jedis.mget(pEventKeyList.toArray(new String[0]));
                for (String pEvent : pEventList) {
                    MqttPublishEvent event = JSON.parseObject(pEvent, MqttPublishEvent.class);
                    if (event != null) {
                        allPublishEvents.add(event);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        return allPublishEvents;
    }

    public List<MqttEvent> retrieveOutdatedPublishMessage(long lifecycle) {
        List<MqttEvent> outdatedPublishEvents = new ArrayList<MqttEvent>();
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            long max = System.currentTimeMillis() - lifecycle;
            Set<String> outdateMembers = jedis.zrangeByScore(INDEX_PUBLISH_MESSAGE_CREATETIME, 0,max);
            List<String> result1 = jedis.mget(outdateMembers.toArray(new String[0]));
            if (result1.size() > 0) {
                for (String item : result1) {
                    MqttPublishEvent mqttPublishEvent = JSON.parseObject(item, MqttPublishEvent.class);
                    if (mqttPublishEvent != null) {
                        outdatedPublishEvents.add(mqttPublishEvent);
                    }
                }
            }
        } catch (Exception e) {

        } finally {
            jedis.close();
        }
        return outdatedPublishEvents;
    }

    @Override
    public MqttEvent retrievePublishedMessage(String publishKey) {
        try {
            String tableKey = TABLE_PUBLISH_EVENTS + SEPARATOR + publishKey + SEPARATOR + "value";
            return (MqttEvent) get(tableKey, MqttPublishEvent.class);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public void removePublishedMessage(String publishKey) {
        Jedis jedis = null;
        try {
            String tableKey = TABLE_PUBLISH_EVENTS + SEPARATOR + publishKey + SEPARATOR + "value";
            del(tableKey);
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.zrem(INDEX_PUBLISH_MESSAGE_CREATETIME, publishKey);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    @Override
    public void removePublishedMessageByClientId(String clientId) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            ScanParams scanParams = new ScanParams();
            String tableKey = TABLE_PUBLISH_EVENTS + SEPARATOR + clientId + "*" + SEPARATOR + "value";
            scanParams.match(tableKey);
            scanParams.count(100);
            //
            ScanResult<String> publishKeys = jedis.scan("0", scanParams);
            List<String> result1 = publishKeys.getResult();
            if (result1.size() > 0) {
                for (String publishKey : result1) {
                    jedis.del(publishKey);
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    @Override
    public List<MqttEvent> retrievePublishedMessageByBatch(int batchSize) {
        return null;
    }

    /****************************  Subscriptions **********************************/
    @Override
    public void addSubscription(Subscription subscription) {
        String clientId = subscription.getClientId();
        String topic = subscription.getTopic();
        String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + clientId + SEPARATOR + topic + SEPARATOR + "value";
        String tableValue = JSON.toJSONString(subscription, false);
        try {
            save(tableKey, tableValue);
        } catch (RedisStorageException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public List<Subscription> retrieveAllSubscriptions() {
        try {
            String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + "*";
            return retrieveSubscriptionWithFuzzy(tableKey);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return new ArrayList<>();
    }

    @Override
    public List<Subscription> retrieveSubscriptionsByClientId(String clientId) {
        try {
            String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + clientId + SEPARATOR + "*" + SEPARATOR + "value";
            return retrieveSubscriptionWithFuzzy(tableKey);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return new ArrayList<>();
    }

    @Override
    public List<Subscription> retrieveSubscriptionByTopic(String topic) {
        try {
            String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + "*" + SEPARATOR + topic + SEPARATOR + "value";
            return retrieveSubscriptionWithFuzzy(tableKey);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return new ArrayList<>();
    }

    @Override
    public void removeSubscription(String clientId, String topic) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + clientId + SEPARATOR + topic + SEPARATOR + "value";
            jedis.del(tableKey);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    @Override
    public Subscription retrieveSubscription(String clientId, String topic) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + clientId + SEPARATOR + topic + SEPARATOR + "value";
            return (Subscription)get(tableKey, Subscription.class);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        return null;
    }

    @Override
    public void removeSubscriptionByClientId(String clientId) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            List<Subscription> subList = retrieveSubscriptionsByClientId(clientId);
            for (Subscription subscription : subList) {
                String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + clientId + SEPARATOR + subscription.getTopic() + SEPARATOR + "value";
                System.out.println(tableKey);
                jedis.del(tableKey);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    @Override
    public void updateSubscription(Subscription subscription) {
        String clientId = subscription.getClientId();
        String topic = subscription.getTopic();
        String tableKey = TABLE_SUBSCRIPTIONS + SEPARATOR + clientId + SEPARATOR + topic + SEPARATOR + "value";
        String tableValue = JSON.toJSONString(subscription, false);
        try {
            save(tableKey, tableValue);
        } catch (RedisStorageException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    //////////////////////////////// Store Message /////////////////////////////////////
    private static final String INDEX_STORE_MESSAGE_TOPIC = TABLE_STORE_MESSAGE + "." + "index";
    private static final String TABLE_CLIENT_MSG = RedisConstants.TABLE_CLIENT_MESSAGE;
    @Override
    public MqttStoreMessage addStoreMessage(MqttPublishEvent mqttPublishEvent) {
        Jedis jedis = null;
        try {
            MqttStoreMessage mqttStoreMessage = MqttUtil.convertToMqttStoreMessage(mqttPublishEvent);
            jedis = jedisConnectionManager.getPersistConnection();
            long storeMsgId = storeMessageIdGenerator.incrementAndGet();
            mqttStoreMessage.setStoreMsgId(storeMsgId);
            String tableKey = TABLE_STORE_MESSAGE + SEPARATOR + storeMsgId + SEPARATOR + "value";
            String tableValue = JSON.toJSONString(mqttStoreMessage, false);
            save(tableKey, tableValue);
            // Process index here
            String topic = mqttStoreMessage.getTopic();
            String indexKey = INDEX_STORE_MESSAGE_TOPIC + topic;
            jedis.sadd(indexKey, Long.toString(storeMsgId));
            return mqttStoreMessage;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        return null;
    }

    @Override
    public List<MqttStoreMessage> retrieveStoreMessage(String topic) {
        List<MqttStoreMessage> storeMessagesList = new ArrayList<MqttStoreMessage>();
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String indexKey = INDEX_STORE_MESSAGE_TOPIC + topic;
            Set<String> storeMsgIds = jedis.smembers(indexKey);
            if (storeMsgIds.size() > 0) {
                List<String> storeMsgList = jedis.mget(storeMsgIds.toArray(new String[0]));
                for (String storeMsg : storeMsgList) {
                    MqttStoreMessage storeMessage = JSON.parseObject(storeMsg, MqttStoreMessage.class);
                    storeMessagesList.add(storeMessage);
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        return storeMessagesList;
    }

    public void ackStoreMessage(String clientId, long storeMsgId) {
        try {
            String tableKey = TABLE_CLIENT_MSG + SEPARATOR + clientId + SEPARATOR + storeMsgId;
            String value = Long.toString(new Date().getTime());
            save(tableKey, value);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
        }
    }

    @Override
    public boolean alreadyAck(String clientId, long storeMsgId) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String tableKey = TABLE_CLIENT_MSG + SEPARATOR + clientId + SEPARATOR + storeMsgId;
            String result = jedis.get(tableKey);
            if (null != result) {
                return true;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
        }
        return false;
    }

    public List<MqttStoreMessage> retrieveUnackStoreMessage(String clientId, String topic) {
        List<MqttStoreMessage> unackStoreMessagesList = new ArrayList<MqttStoreMessage>();
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            // Get those message already ack
            String tableKey = TABLE_CLIENT_MSG + SEPARATOR + clientId + SEPARATOR + "*";
            ScanParams scanParams = new ScanParams();
            scanParams.match(tableKey);
            scanParams.count(100);
            ScanResult<String> scanResult = jedis.scan("0", scanParams);
            List<String> ackSets = scanResult.getResult();
            // Get all existing messages
            String indexKey = INDEX_STORE_MESSAGE_TOPIC + topic;
            Set<String> storeMsgIds = jedis.smembers(indexKey);
            if (storeMsgIds.size() > 0) {
                List<String> storeMsgKeys = new ArrayList<>(storeMsgIds.size());
                for (String storeMsgId : storeMsgIds) {
                    String storeMsgKey = TABLE_STORE_MESSAGE + SEPARATOR + storeMsgId + SEPARATOR + "value";
                    storeMsgKeys.add(storeMsgKey);
                }
                List<String> storeMsgList = jedis.mget(storeMsgKeys.toArray(new String[0]));
                for (String storeMsg : storeMsgList) {
                    MqttStoreMessage storeMessage = JSON.parseObject(storeMsg, MqttStoreMessage.class);
                    if (storeMessage != null) {
                        String _tableKey = TABLE_CLIENT_MSG + SEPARATOR + clientId + SEPARATOR + storeMessage.getStoreMsgId();
                        if (ackSets != null && !ackSets.contains(_tableKey)) {
                            unackStoreMessagesList.add(storeMessage);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        return unackStoreMessagesList;
    }

    @Override
    public MqttStoreMessage retrieveStoreMessage(long storeMsgId) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String tableKey = TABLE_STORE_MESSAGE + SEPARATOR + storeMsgId + SEPARATOR + "value";
            String result = jedis.get(tableKey);
            MqttStoreMessage mqttStoreMessage = JSON.parseObject(result, MqttStoreMessage.class);
            mqttStoreMessage.setStoreMsgId(storeMsgId);
            return mqttStoreMessage;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        return null;
    }

    @Override
    public void removeStoreMessage(String clientId, int msgId, String topic) {

    }

    @Override
    public void removeStoreMessage(long storeMsgId) {
        try {
            String tableKey = TABLE_STORE_MESSAGE + SEPARATOR + storeMsgId + SEPARATOR + "value";
            del(tableKey);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
        }
    }

    //////////////////////////////////////////////////////// Notify Subscriber ///////////////////////////////////////////////////////////
    private static final String INDEX_NOTIFY_EVENT_COUNTER_ACK = "indexNotifyEventCounterAck";
    private static final String INDEX_NOTIFY_EVENT_COUNTER_CREATETIME = "indexNotifyEventCounterCreateTime";

    @Override
    public void loggingNotifyEvent(long storeMsgId) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String indexKey = INDEX_NOTIFY_EVENT_COUNTER_ACK;
            jedis.zadd(indexKey, 0, Long.toString(storeMsgId));
            indexKey = INDEX_NOTIFY_EVENT_COUNTER_CREATETIME;
            jedis.zadd(indexKey, 0,  Long.toString(new Date().getTime()) + SEPARATOR + Long.toString(storeMsgId));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    private String transactionIncr(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
//            jedis.watch(key);
            Transaction tx = jedis.multi();
            jedis.incr(key);
            List<Object> result =  tx.exec();
            return result == null ? null : (String)result.get(1);
        } catch (Exception e) {

        } finally {
            jedis.disconnect();
        }
        return null;
    }

    private String compareAndSet(String key, String current, String next) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.watch(key);
            Transaction tx = jedis.multi();
//            jedis.incr(key);
            jedis.set(key, next);
            List<Object> result =  tx.exec();
            return result == null ? null : (String)result.get(1);
        } catch (Exception e) {

        } finally {
            jedis.disconnect();
        }
        return null;
    }

    @Override
    public void registerNodeInfo() {
        // implement in zookeeper in next version for monitor
    }

    @Override
    public int getOnlineNodeCount() {
        return 1;
    }

    @Override
    public void ackNotifyEvent(long storeMsgId) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String indexKey = INDEX_NOTIFY_EVENT_COUNTER_ACK;
            jedis.zincrby(indexKey, 1, Long.toString(storeMsgId));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
    }

    @Override
    public Set<Long> getUnackNotifyEventWithPeriodInSecond(int totalOnlineAcker, int period) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String indexKey = INDEX_NOTIFY_EVENT_COUNTER_ACK;
            Set<String> result1 = jedis.zrangeByScore(indexKey, 0, totalOnlineAcker - 1);
            Set<String> tmpResult = jedis.zrangeByLex(indexKey, "[" + 0, "(" + (System.currentTimeMillis() - period * 1000));
            Set<Long> result2 = new HashSet<>();
            for (String tmpString : tmpResult) {
                String tmpStoreMsgId = tmpString.substring(0, tmpString.indexOf(":"));
                if (result1.contains(tmpStoreMsgId)) {
                    result2.add(Long.parseLong(tmpStoreMsgId));
                }
            }
            return result2;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            jedis.close();
        }
        return null;
    }

    //////////////////////////////////////////////////////  Private Methods ///////////////////////////////////////////////////////
    private List<Subscription> retrieveSubscriptionWithFuzzy(String tableKey) throws RedisStorageException {
        List<Subscription> allSubscriptions = new ArrayList<Subscription>();
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            ScanParams scanParams = new ScanParams();
            scanParams.match(tableKey);
            scanParams.count(100);
            ScanResult<String> results = jedis.scan("0", scanParams);
            String[] keys = results.getResult().toArray(new String[0]);
            if (keys.length > 0) {
                List<String> values = jedis.mget(keys);
                for (String result : values) {
                    Subscription sub = (Subscription) JSON.parseObject(result, Subscription.class);
                    allSubscriptions.add(sub);
                }
            }
        } catch (Exception e) {
    //            LOGGER.error(e.getMessage(), e);
            throw new RedisStorageException("Fail to retrieve...", e);
        } finally {
            jedis.close();
        }
        return allSubscriptions;
    }

    private void save(String tableKey, String tableValue) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.set(tableKey, tableValue);
        } catch (Exception e) {
            throw new RedisStorageException("Fail to save...", e);
        } finally {
            jedis.close();
        }
    }

    private void del(String key) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.del(key);
        } catch (Exception e) {
            throw new RedisStorageException("Fail to delete...", e);
        } finally {
            jedis.close();
        }
    }

    private Object get(String key, Class clazz) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            String result =  jedis.get(key);
            Object object = JSON.parseObject(result, clazz);
            return object;
        } catch (Exception e) {
            throw new RedisStorageException("Fail to get... ", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void setJedisConnectionManager(JedisConnectionManager jedisConnectionManager) {
        this.jedisConnectionManager = jedisConnectionManager;
    }
}
