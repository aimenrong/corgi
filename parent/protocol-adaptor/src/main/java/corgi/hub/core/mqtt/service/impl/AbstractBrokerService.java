package corgi.hub.core.mqtt.service.impl;

import corgi.hub.core.common.Constants;
import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.HubSession;
import corgi.hub.core.mqtt.bean.MqttSession;
import corgi.hub.core.mqtt.bean.RetainedMessage;
import corgi.hub.core.mqtt.bean.Subscription;
import corgi.hub.core.mqtt.common.*;
import corgi.hub.core.mqtt.dao.BrokerConsumerDao;
import corgi.hub.core.mqtt.dao.BrokerProducerDao;
import corgi.hub.core.mqtt.event.*;
import corgi.hub.core.mqtt.service.IBrokerService;
import corgi.hub.core.mqtt.service.IStorageService;
import corgi.hub.core.mqtt.util.MqttUtil;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Terry LIANG on 2017/1/11.
 */
public abstract class AbstractBrokerService implements IBrokerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBrokerService.class);

    @Resource(name = "redisStorageService")
    protected IStorageService storageService;
    @Resource(name = "kafkaConsumerDao")
    protected BrokerConsumerDao brokerConsumerDao;
    @Resource(name = "kafkaProducerDao")
    protected BrokerProducerDao brokerProducerDao;

    @Override
    public void processConnect(HubContext context, MqttEvent mqttEvent) {
        MqttConnectEvent mqttConnectEvent = (MqttConnectEvent) mqttEvent;

        MqttMessage msg = mqttConnectEvent.getMqttMessage();
        MqttFixedHeader conAckFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.valueOf(MqttConstants.QOS_AT_MOST_ONCE), false, 0);
        MqttConnectVariableHeader variableHeader = (MqttConnectVariableHeader) msg.variableHeader();
        MqttConnectPayload payload = (MqttConnectPayload) msg.payload();
        String clientId = payload.clientIdentifier();
        ServerChannel channel = mqttConnectEvent.getChannel();
        boolean cleanSession = variableHeader.isCleanSession();
        LOGGER.info("processConnect for client " + clientId);
        if (variableHeader.version() != 3 && variableHeader.version() != 4) {
            MqttConnAckMessage badProtocol = new MqttConnAckMessage(conAckFixedHeader, new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false));
            LOGGER.error("CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION");
            channel.write(badProtocol);
            channel.close(false);
            return;
        }

        if (clientId == null || clientId.length() > 23) {
            MqttConnAckMessage okResp = new MqttConnAckMessage(conAckFixedHeader, new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false));
            channel.write(okResp);
            LOGGER.error("CONNECTION_REFUSED_IDENTIFIER_REJECTED");
            return;
        }

        //handle user authentication
        if (variableHeader.hasUserName() && variableHeader.hasPassword()) {
            String pwd = null;
            pwd = payload.password();
            String username = payload.userName();
//            if (!m_authenticator.checkValid(msg.getUsername(), pwd)) {
            MqttConnAckMessage okResp = new MqttConnAckMessage(conAckFixedHeader, new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false));
            channel.write(okResp);
            return;
//            }
        }

        if (context.sessionExist(clientId)) {
            HubSession oldSession = context.getSession(clientId);
            ServerChannel oldSessionChannel = context.getSession(clientId).getChannel();
            boolean oCleanSession = oldSession.isCleanSession();
            if (oCleanSession) {
                context.getSubscriptionManager().removeForClient(clientId);
            }
            context.getSession(clientId).getChannel().close(false);
        }

        HubSession session = new MqttSession(clientId, channel, cleanSession);
        context.putSession(clientId, session);

        int keepAlive = variableHeader.keepAliveTimeSeconds();
        channel.setAttribute(Constants.KEEP_ALIVE, keepAlive);
        channel.setAttribute(Constants.CLEAN_SESSION, cleanSession);
        //used to track the client in the subscription and publishing phases.
        channel.setAttribute(Constants.ATTR_CLIENTID, clientId);

        channel.setIdleTime(Math.round(keepAlive * 1.5f));

        //Handle will flag
        if (variableHeader.isWillFlag()) {
            MqttPublishEvent _pubEvent = new MqttPublishEvent(payload.willTopic(), variableHeader.willQos(), payload.willMessage().getBytes(),
                    variableHeader.isWillRetain(), clientId, channel);
            storageService.storePublishedMessage(_pubEvent);
            brokerProducerDao.produceMessage(context, _pubEvent);
            storageService.removePublishedMessage(MqttUtil.formatPublishKey(_pubEvent.getClientId(), _pubEvent.getMsgId()));
        }
        //handle clean session flag
        if (cleanSession) {
            //remove all prev subscriptions
            //cleanup topic subscriptions
            context.getSubscriptionManager().removeForClient(clientId);
        }

        context.getSubscriptionManager().activateSubscription(clientId);

        MqttConnAckMessage okResp = new MqttConnAckMessage(conAckFixedHeader, new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, true));
        LOGGER.info("processConnect sent OK ConnAck");
        channel.write(okResp);

        // Get the old message from client's subscription
        List<Subscription> oldSubscriptions = storageService.retrieveSubscriptionsByClientId(clientId);
        if (oldSubscriptions.size() > 0) {
            MqttSubscribeEvent subscribeEvent = new MqttSubscribeEvent();
            subscribeEvent.setClientId(clientId);
            subscribeEvent.setChannel(channel);
            subscribeEvent.setEarliest(true);
            for (Subscription oldSub : oldSubscriptions) {
                MqttSubscribeEvent.Couple couple = new MqttSubscribeEvent.Couple(oldSub.getRequestedQos(), oldSub.getTopic());
                subscribeEvent.addSubscription(couple);
                // Get retained messages here
                //scans retained messages to be published to the new subscription

                Collection<RetainedMessage> messages = storageService.searchMatchingRetainedMessage(new IMatchingCondition() {
                    public boolean match(String key) {
                        return  MqttSubscriptionManager.matchTopics(key, oldSub.getTopic());
                    }
                });

                for (RetainedMessage storedMsg : messages) {
                    //fire the as retained the message
                    MqttPublishMessage pubMessage = MqttUtil.createPublishMessage(storedMsg.getTopic(), storedMsg.getQos(), storedMsg.getPayload(), true, 0);
                    channel.write(pubMessage);
                }

            }

            if (!brokerConsumerDao.topicExist(clientId, oldSubscriptions.get(0).getTopic())) {
                brokerConsumerDao.subscribeMessage(subscribeEvent);
            }

        }

        if (!cleanSession) {
            //force the republish of stored QoS2
            republishStored(context, clientId);
        }
    }

    private void republishStored(HubContext context, String clientId) {
        List<MqttEvent> eventList = storageService.retrivePublishedMessageByClientId(clientId);
        if (eventList == null) {
            LOGGER.info("republishStored, no stored publish events");
            return;
        }
        List<MqttPublishEvent> publishEventList = new ArrayList<>(eventList.size());
        eventList.stream().forEach(event -> {
            brokerProducerDao.produceMessage(context, event);
        });
    }

    @Override
    public void processDisconnect(HubContext context, MqttEvent mqttEvent) {
        MqttDisconnectEvent mqttDisconnectEvent = (MqttDisconnectEvent) mqttEvent;
        String clientId = mqttDisconnectEvent.getClientId();
        boolean cleanSession = mqttDisconnectEvent.isCleanSession();
        ServerChannel channel = context.getSession(clientId).getChannel();
        List<Subscription> allSubscriptions = storageService.retrieveSubscriptionsByClientId(clientId);
        for (Subscription subscription : allSubscriptions) {
            if (brokerConsumerDao.topicExist(clientId, subscription.getTopic())) {
                brokerConsumerDao.cancelConsumer(clientId, subscription.getTopic());
            }
        }
        if (cleanSession) {
            //cleanup topic subscriptions
            context.getSubscriptionManager().removeForClient(clientId);
        }
        context.remove(clientId);
        channel.close(true);

        //de-activate the subscriptions for this ClientID
        context.getSubscriptionManager().deactivateSubscription(clientId);
    }

    private void subscribeSingleTopic(HubContext context, Subscription newSubscription, final String topic) {
        context.getSubscriptionManager().removeSubscription(newSubscription.getClientId(), newSubscription.getTopic());
        storageService.removeSubscription(newSubscription.getClientId(), topic);
        context.getSubscriptionManager().add(newSubscription);

        //scans retained messages to be published to the new subscription
        Collection<RetainedMessage> messages = storageService.searchMatchingRetainedMessage(new IMatchingCondition() {
            public boolean match(String key) {
                return  MqttSubscriptionManager.matchTopics(key, topic);
            }
        });

        for (RetainedMessage storedMsg : messages) {
            //fire the as retained the message
            MqttPublishMessage pubMessage = MqttUtil.createPublishMessage(storedMsg.getTopic(), storedMsg.getQos(), storedMsg.getPayload(), true, 0);
            context.getSession(newSubscription.getClientId()).getChannel().write(pubMessage);
        }
    }

    @Override
    public void processSubscribe(HubContext context, MqttEvent mqttEvent) {
        MqttSubscribeEvent mqttSubscribeEvent = (MqttSubscribeEvent) mqttEvent;
        String clientId = mqttSubscribeEvent.getClientId();
        boolean cleanSession = mqttSubscribeEvent.isCleanSession();
        ServerChannel channel = context.getSession(clientId).getChannel();
        for (MqttSubscribeEvent.Couple req : mqttSubscribeEvent.subscriptions()) {
            int qos = req.getQos();
            Subscription newSubscription = new Subscription(clientId, req.getTopic(), qos, cleanSession);
            subscribeSingleTopic(context, newSubscription, req.getTopic());
        }
        if (!brokerConsumerDao.topicExist(clientId, mqttSubscribeEvent.subscriptions().get(0).getTopic())) {
            brokerConsumerDao.subscribeMessage(mqttSubscribeEvent);
        }
        int ackType[] = new int[mqttSubscribeEvent.subscriptions().size()];
        //TODO by now it handles only QoS 0 messages
        for (int i = 0; i < mqttSubscribeEvent.subscriptions().size(); i++) {
            ackType[i] = 0;
        }
        //ack the client
        MqttSubAckMessage ackMessage = MqttUtil.createSubAckMessage(mqttSubscribeEvent.getMessageId(), ackType);
        channel.write(ackMessage);
    }

    @Override
    public void processUnsubscribe(HubContext context, MqttEvent mqttEvent) {
        MqttUnsubscribeEvent mqttUnsubscribeEvent = (MqttUnsubscribeEvent) mqttEvent;
        String clientId = mqttUnsubscribeEvent.getClientId();
        List<String> topics = mqttUnsubscribeEvent.getTopics();
        int msgId = mqttUnsubscribeEvent.getMsgId();
        ServerChannel serverChannel = context.getSession(clientId).getChannel();
        for (String topic : topics) {
            context.getSubscriptionManager().removeSubscription(topic, clientId);
            storageService.removeSubscription(clientId, topic);
            // Cancel in asyn way
            if (brokerConsumerDao.topicExist(clientId, topic)) {
                brokerConsumerDao.cancelConsumer(clientId, topic);
            }
        }

        //ack the client
        MqttMessage ackMessage = MqttUtil.createGeneralMessage(MqttMessageType.UNSUBACK, msgId);
        serverChannel.write(ackMessage);
    }

    @Override
    public void processPubRec(HubContext context, MqttEvent mqttEvent) {
        MqttPubRecEvent mqttPubRecEvent = (MqttPubRecEvent) mqttEvent;
        String clientId = mqttPubRecEvent.getClientId();
        int msgId = mqttPubRecEvent.getMsgId();
        MqttMessage pubRelMsg = MqttUtil.createGeneralMessage(MqttMessageType.PUBREL, msgId);
        context.getSession(clientId).getChannel().write(pubRelMsg);
    }

    @Override
    public void processPubRel(HubContext context, MqttEvent mqttEvent) {
        IKafkaCallback<MqttPubRelEvent> callback = new ProducerQos2Callback();
        brokerProducerDao.<MqttPubRelEvent>produceMessageWithCallback(context, (MqttPubRelEvent)mqttEvent, callback);
    }

    @Override
    public void processPubAck(HubContext context, MqttEvent mqttEvent) {
        MqttPubAckEvent mqttPubAckEvent = (MqttPubAckEvent) mqttEvent;
        String clientId = mqttPubAckEvent.getClientId();
        int msgId = mqttPubAckEvent.getMessageId();
        storageService.removePublishedMessage(MqttUtil.formatPublishKey(clientId, msgId));
    }

    @Override
    public void processPubComp(HubContext context, MqttEvent mqttEvent) {
        // Do nothing
    }

    private void sendPubRec(ServerChannel channel, String clientId, int msgId) {
        MqttMessage pubRecMessage = MqttUtil.createGeneralMessage(MqttMessageType.PUBREC, msgId);
        channel.write(pubRecMessage);
    }

    private void sendPubAck(ServerChannel publishChannel, MqttPubAckEvent event) {
        // Remove Qos1 message in storage
        MqttMessage pubAckMessage = MqttUtil.createGeneralMessage(MqttMessageType.PUBACK, event.getMessageId());
        try {
            publishChannel.write(pubAckMessage);
        }catch(Throwable t) {
            t.printStackTrace();
        }
    }
}
