package corgi.hub.core.mqtt;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import corgi.hub.core.mqtt.common.HubContext;
import corgi.hub.core.mqtt.common.RedisSubscriptionProcessor;
import corgi.hub.core.mqtt.event.*;
import corgi.hub.core.mqtt.common.JedisConnectionManager;
import corgi.hub.core.mqtt.exception.RedisStorageException;
import corgi.hub.core.mqtt.service.IBrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Terry LIANG on 2016/12/20.
 */
@Component
public class MqttEventProcesser implements EventHandler<CoreEvent> {
    private Logger LOGGER = LoggerFactory.getLogger(MqttEventProcesser.class);

    private RingBuffer<CoreEvent> ringBuffer;
    private ExecutorService disruptorExecutor;
    private BatchEventProcessor<CoreEvent> batchEventProcessor;

    @Autowired
    private JedisConnectionManager jedisConnectionManager;

    @Resource(name = "mqttContext")
    private HubContext hubContext;

    @Resource(name = "redisBrokerService")
    private IBrokerService brokerService;

    @Resource(name = "redisSubscriptionProcessor")
    private RedisSubscriptionProcessor redisSubscriptionProcessor;

    private ThreadPoolTaskExecutor executor;

    @PostConstruct
    public void init() {
        disruptorExecutor = Executors.newFixedThreadPool(1);

        ringBuffer = new RingBuffer<CoreEvent>(CoreEvent.EVENT_FACTORY, 1024 * 32);

        SequenceBarrier barrier = ringBuffer.newBarrier();
        batchEventProcessor = new BatchEventProcessor<CoreEvent>(ringBuffer, barrier, this);
        //TODO in a presentation is said to don't do the followinf line!!
        ringBuffer.setGatingSequences(batchEventProcessor.getSequence());
        disruptorExecutor.submit(batchEventProcessor);
        //
        executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(100);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(10000);
        executor.setKeepAliveSeconds(300);
        executor.initialize();
    }

    private void enLocalQueue(MqttEvent msgEvent) {
        System.out.println("disruptorPublish publishing event " + msgEvent);
        long sequence = ringBuffer.next();
        CoreEvent<MqttEvent> event = ringBuffer.get(sequence);
        event.setEvent(msgEvent);
        ringBuffer.publish(sequence);
    }

    /**
     * Local event processer
     * @param coreEvent event to be processed
     * @param l unused
     * @param b  unused
     * @throws Exception
     */
    @Override
    public void onEvent(CoreEvent coreEvent, long l, boolean b) throws Exception {
        Object event = coreEvent.getEvent();
        if (null == event) {
            LOGGER.error("event is null");
            return;
        }
        MqttEvent mqttEvent = (MqttEvent) event;
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if (event instanceof MqttConnectEvent) {
                    brokerService.processConnect(hubContext, mqttEvent);
                } else if (event instanceof MqttDisconnectEvent) {
                    brokerService.processDisconnect(hubContext, mqttEvent);
                } else if (event instanceof MqttSubscribeEvent) {
                    ((MqttSubscribeEvent)mqttEvent).setContext(hubContext);
                    brokerService.processSubscribe(hubContext, mqttEvent);
                } else if (event instanceof MqttUnsubscribeEvent) {
                    brokerService.processUnsubscribe(hubContext, mqttEvent);
                } else if (event instanceof MqttPubRecEvent) {
                    brokerService.processPubRec(hubContext, mqttEvent);
                } else if (event instanceof MqttPublishEvent) {
                    brokerService.processPublish(hubContext, mqttEvent);
                } else if (event instanceof MqttPubRelEvent) {
                    brokerService.processPubRel(hubContext, mqttEvent);
                } else if (event instanceof MqttPubAckEvent) {
                    brokerService.processPubAck(hubContext, mqttEvent);
                }
            }
        });
    }
    public void enqueue(MqttEvent event) {
        enLocalQueue(event);
    }
}
