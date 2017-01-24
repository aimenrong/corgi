package corgi.hub.core.mqtt.listener.impl;

import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.common.MqttConstants;
import corgi.hub.core.mqtt.listener.QueueListenable;
import corgi.hub.core.mqtt.listener.QueueListener;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
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
 * Process downlink message
 * Created by Terry LIANG on 2017/1/19.
 */
@Component("InternalExclusiveQueueListenable")
public class InternalExclusiveQueueListenable extends GeneralQueueListenable {
    private final Logger LOGGER = LoggerFactory.getLogger(InternalPublishQueueListenable.class);
    private String name = InternalExclusiveQueueListenable.class.getName();

    private static final String EXCLUSIVE_QUEUE_PREFIX = "exclusiveQueue-";
    private String queueName = MqttConstants.INTERNAL_PUBLISH_QUEUE;
    @Autowired
    private JedisConnectionManager jedisConnectionManager;
    @Resource(name = "redisStorageService")
    private IStorageService storageService;
    @Resource(name = "queueThreadPoolExecutor")
    private ThreadPoolTaskExecutor queueWorkerThreadPoolExecutor;
    @Value("${app.internal.exclusive.queue.listen.time.second:30}")
    private int queueListenTimeOutInSecond = 30;
    @Autowired
    private HubContext context;
    @Resource(name = "InternalExclusiveQueueListener")
    private QueueListener defaultInternalExclusiveQueueListener;

    @PostConstruct
    private void init() {
        InternalExclusiveQueueListenable.super.queueListenerMap.put(defaultInternalExclusiveQueueListener.getListenerName(), defaultInternalExclusiveQueueListener);
        this.queueName = EXCLUSIVE_QUEUE_PREFIX + "-" + context.getNodeName();
        queueWorkerThreadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Jedis jedis = null;
                    jedis = jedisConnectionManager.getPersistConnection();
                    LOGGER.info("Start to listen exclusive queue {}", queueName);
                    while (true) {
                        // 1. retrieve the publish message
                        List<String> msgs = jedis.brpop(queueListenTimeOutInSecond, queueName);
                        if (msgs == null || msgs.size() < 2)
                            continue;
                        long storeMsgId = Long.parseLong(msgs.get(1));
                        LOGGER.info("Receive one message with storeMsgId {} from exclusive queue", storeMsgId);
                        for (Map.Entry<String, QueueListener> entry : InternalExclusiveQueueListenable.super.queueListenerMap.entrySet()) {
                            entry.getValue().onQueueMessage(storeMsgId);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void setListenableName(String name) {

    }

    @Override
    public String getListenableName() {
        return this.name;
    }
}
