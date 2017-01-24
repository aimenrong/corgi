package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.common.MqttConstants;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.exception.RedisStorageException;
import corgi.hub.core.mqtt.listener.QueueListenable;
import corgi.hub.core.mqtt.listener.QueueListener;
import corgi.hub.core.mqtt.service.IStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * Created by Terry LIANG on 2017/1/19.
 */
@Component("InternalPublishQueueListenable")
public class InternalPublishQueueListenable extends GeneralQueueListenable {
    private final Logger LOGGER = LoggerFactory.getLogger(InternalPublishQueueListenable.class);
    private String name = InternalPublishQueueListenable.class.getName();

    private String queueName = MqttConstants.INTERNAL_PUBLISH_QUEUE;
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Resource(name = "queueThreadPoolExecutor")
    private ThreadPoolTaskExecutor queueWorkerThreadPoolExecutor;
    @Value("${app.internal.publish.queue.listen.time.second:30}")
    private int queueListenTimeOutInSecond = 30;
    @Resource(name = "InternalPublishQueueListener")
    private QueueListener defaultInternalPublishQueueListener;

    @PostConstruct
    private void init() {
        try {
            InternalPublishQueueListenable.super.queueListenerMap.put(defaultInternalPublishQueueListener.getListenerName(), defaultInternalPublishQueueListener);
            queueWorkerThreadPoolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    Jedis jedis = null;
                    jedis = jedisConnectionManager.getPersistConnection();
                    while (true) {
                        try {
                            // 1. retrieve the publish message
                            List<String> msgs = jedis.brpop(queueListenTimeOutInSecond, queueName);
                            if (msgs == null || msgs.size() < 2)
                                continue;
                            String publishKey = msgs.get(1);

                            for (Map.Entry<String, QueueListener> entry : InternalPublishQueueListenable.super.queueListenerMap.entrySet()) {
                                entry.getValue().onQueueMessage(publishKey);
                            }
                            LOGGER.info("Receive one mssage with publishKey {} from internal queue", publishKey);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
        }
    }

    @Override
    public String getListenableName() {
        return this.name;
    }
}
