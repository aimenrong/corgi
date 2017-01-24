package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.exception.NoConsumerAvailableException;
import corgi.hub.core.mqtt.listener.QueueListenable;
import corgi.hub.core.mqtt.listener.QueueListener;
import corgi.hub.core.mqtt.service.IQueueService;
import corgi.hub.core.mqtt.service.IStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Terry LIANG on 2017/1/19.
 */
@Component("VirtualTopicQueueListenable")
public class VirtualTopicQueueListenable extends GeneralQueueListenable implements IQueueService {
    private final Logger LOGGER = LoggerFactory.getLogger(VirtualTopicQueueListenable.class);
    private String name = VirtualTopicQueueListenable.class.getName();

    @Resource(name = "queueThreadPoolExecutor")
    private ThreadPoolTaskExecutor queueWorkerThreadPoolExecutor;
    @Value("${app.virtualTopic.listen.time.second:30}")
    private int queueListenTimeOutInSecond = 30;
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Autowired
    private HubContext context;
    private Map<String, Future> virtualTopicConsumerThreadMap = new ConcurrentHashMap<>();
    @Resource(name = "VirtualTopicQueueListener")
    private QueueListener defaultVirtualTopicQueueListener;
    private Lock lock = new ReentrantLock();

    @PostConstruct
    private void init() {
        VirtualTopicQueueListenable.super.queueListenerMap.put(defaultVirtualTopicQueueListener.getListenerName(), defaultVirtualTopicQueueListener);
    }

    @Override
    public void listenQueue(Subscription subscription) {
        String queueName = subscription.getTopic();
        if (!virtualTopicConsumerThreadMap.containsKey(queueName)) {
            LOGGER.info("{} listen new queue {}", subscription.getClientId(), queueName);
            Future future = queueWorkerThreadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Jedis jedis = null;
                    try {
                        jedis = jedisConnectionManager.getPersistConnection();
                        while (true) {
                            // 1. retrieve the publish message
                            List<String> msgs = jedis.brpop(queueListenTimeOutInSecond, queueName);
                            try {
                                lock.lock();
                                if (msgs == null || msgs.size() < 2)
                                    continue;
                                long storeMsgId = Long.parseLong(msgs.get(1));
                                LOGGER.info("Receive virtual topic message with message ID {} ", storeMsgId);
                                for (Map.Entry<String, QueueListener> entry : VirtualTopicQueueListenable.super.queueListenerMap.entrySet()) {
                                    entry.getValue().onQueueMessage(storeMsgId);
                                }
                                storageService.removeStoreMessage(storeMsgId);
                            } catch (NoConsumerAvailableException e1) {
                                virtualTopicConsumerThreadMap.remove(queueName);
                            } finally {
                                lock.unlock();
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    } finally {
                        if (jedis != null) {
                            jedis.close();
                        }
                    }
                }
            });
            putShard(queueName, subscription);
            virtualTopicConsumerThreadMap.put(queueName, future);
        } else {
            // Add into shard
            LOGGER.info("add new subscription {} into queue {}", subscription.getClientId(), queueName);
            try {
                lock.lock();
                putShard(queueName, subscription);
            } finally {
                lock.unlock();
            }
        }
    }

    private void putShard(String queueName, Subscription subscription) {
        VirtualTopicQueueListener virtualTopicQueueListener = (VirtualTopicQueueListener) defaultVirtualTopicQueueListener;
        virtualTopicQueueListener.putShard(queueName, subscription);
    }

    @Override
    public void unlistenQueue(Subscription subscription) {
        try {
            lock.lock();
            String queueName = subscription.getTopic();
            VirtualTopicQueueListener virtualTopicQueueListener = (VirtualTopicQueueListener) defaultVirtualTopicQueueListener;
            virtualTopicQueueListener.removeShard(queueName, subscription);
            if (virtualTopicQueueListener.size(queueName) == 0) {
                virtualTopicConsumerThreadMap.remove(queueName);
            }
            LOGGER.info("{} unlisten queue {}", subscription.getClientId(), subscription.getTopic());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void pushQueueMessage(String queue, long storeMsgId) {
        Jedis jedis = null;
        try {
            jedis = jedisConnectionManager.getPersistConnection();
            jedis.lpush(queue, Long.toString(storeMsgId));
            LOGGER.info("Push the message {} to {}", storeMsgId, queue);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public void setListenableName(String name) {

    }

    @Override
    public String getListenableName() {
        return name;
    }
}
