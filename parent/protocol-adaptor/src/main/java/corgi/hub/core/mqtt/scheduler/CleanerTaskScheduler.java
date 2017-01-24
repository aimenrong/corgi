package corgi.hub.core.mqtt.scheduler;

import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.ISubscriptionManager;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.common.MqttConstants;
import corgi.hub.core.mqtt.dao.BrokerProducerDao;
import corgi.hub.core.mqtt.event.MqttEvent;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.exception.RedisStorageException;
import corgi.hub.core.mqtt.service.IQueueService;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Set;

/**
 * Created by Terry LIANG on 2017/1/12.
 * Process Qos 1 and Qos 2 message
 * Qos 0 message processing is in RedisSubscriptionProcessor
 */
@Component
public class CleanerTaskScheduler {
    private static Logger LOGGER = LoggerFactory.getLogger(CleanerTaskScheduler.class);
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    private int batchSize = 100;
    private int lockPeriodInSecond = 100;
    @Resource(name = "redisStorageService")
    private IStorageService storageService;

    private String internalTopic;
    @Value("${app.publishEventLifeCycle.second:60}")
    private long publishEventLifeCyclePeriodInSecond = 60;
    @Resource(name = "redisProducerDao")
    private BrokerProducerDao brokerProducerDao;
    @Value("${app.unack.notify.event.lifecycle:5}")
    private int unackNotifyEventLifecycleInSecond;
    @Autowired
    private ISubscriptionManager subscriptionManager;
    @Autowired
    private IQueueService queueService;
    @Autowired
    private HubContext context;
    @Resource(name = "queueThreadPoolExecutor")
    private ThreadPoolTaskExecutor queueWorkerTHreadPoolExecutor;


    @Scheduled(fixedDelay = 1000)
    public void scanOutdatedUnackNotifyEvent() {
        Set<Long> resultSet = storageService.getUnackNotifyEventWithPeriodInSecond(1, unackNotifyEventLifecycleInSecond);
        for (Long storeMsgId : resultSet) {
            try {
                notifySubscriber(Long.toString(storeMsgId));
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private void notifySubscriber(String message) throws RedisStorageException {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.publish(internalTopic, message);
        } catch (Exception e) {
            throw new RedisStorageException(String.format("Fail to publish %s %s... ", internalTopic, message), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void scanOutdatedPublishMessage() {
//        LOGGER.debug("Start to scan outdated publish message");
        List<MqttEvent> mqttEventList = storageService.retrieveOutdatedPublishMessage(publishEventLifeCyclePeriodInSecond * 1000);
        for (MqttEvent mqttEvent : mqttEventList) {
            MqttPublishEvent mqttPublishEvent = (MqttPublishEvent) mqttEvent;
            brokerProducerDao.repushPublishEvent(MqttUtil.formatPublishKey(mqttPublishEvent.getClientId(), mqttPublishEvent.getMsgId()));
        }
    }

//    @Scheduled(fixedDelay =  30000)
    public void housekeepObsoleteStoreMessage() throws InterruptedException {
        Jedis jedis = null;
        try {
            jedis.setnx("scanLock", Long.toString(System.currentTimeMillis()));
            jedis.expire("scanLock", lockPeriodInSecond);

            jedis.del("scanLock");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

}
