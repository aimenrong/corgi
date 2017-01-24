package corgi.hub.core.mqtt;

import corgi.hub.core.common.Constants;
import corgi.hub.core.mqtt.bean.HubSession;
import corgi.hub.core.mqtt.event.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Terry LIANG on 2016/12/23.
 */
@Component
public class MqttServerHandler extends ChannelInboundHandlerAdapter {
    private Logger LOGGER = LoggerFactory.getLogger(MqttServerHandler.class);

    private Map<ChannelHandlerContext, NettyChannel> channelMapper = new HashMap<ChannelHandlerContext, NettyChannel>();
    @Autowired
    private MqttEventProcesser mqttEventProcesser;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        MqttMessage mqttMessage = (MqttMessage) message;
        MqttFixedHeader fixedHeader = mqttMessage.fixedHeader();

        MqttMessageType messageType = fixedHeader.messageType();
        NettyChannel channel = null;
        synchronized(channelMapper) {
            if (!channelMapper.containsKey(ctx)) {
                channelMapper.put(ctx, new NettyChannel(ctx));
            }
            channel = channelMapper.get(ctx);
        }
        String clientId = null;
        boolean cleanSession = false;
        int keepAlived = 0;
        if ( !MqttMessageType.CONNECT.equals(messageType)) {
            clientId = (String)channel.getAttribute(Constants.ATTR_CLIENTID);
            cleanSession = (Boolean) channel.getAttribute(Constants.CLEAN_SESSION);
            keepAlived = (Integer) channel.getAttribute(Constants.KEEP_ALIVE);
        }
        BaseEvent baseEvent = null;
        switch (messageType) {
            case CONNECT:
                LOGGER.info("Receive CONNECT message");
                baseEvent = new MqttConnectEvent();
                break;
            case SUBSCRIBE:
                LOGGER.info("Receive SUBSCRIBE message");
                baseEvent = new MqttSubscribeEvent(mqttMessage);
                MqttMessageIdVariableHeader subscribeVariableHeader = (MqttMessageIdVariableHeader)mqttMessage.variableHeader();
                ((MqttSubscribeEvent)baseEvent).setMessageId(subscribeVariableHeader.messageId());
                ((MqttSubscribeEvent)baseEvent).setEarliest(false);
                break;
            case UNSUBSCRIBE:
                LOGGER.info("Receive UNSUBSCRIBE message");
                MqttMessageIdVariableHeader unsubscribeVariableHeader = (MqttMessageIdVariableHeader)mqttMessage.variableHeader();
                baseEvent = new MqttUnsubscribeEvent(mqttMessage);
                ((MqttUnsubscribeEvent)baseEvent).setMsgId(unsubscribeVariableHeader.messageId());
                break;
            case DISCONNECT:
                LOGGER.info("Receive DISCONNECT message");
                baseEvent = new MqttDisconnectEvent();
                break;
            case PUBLISH:
                LOGGER.info("Receive PUBLISH message");
                baseEvent = new MqttPublishEvent();
                MqttFixedHeader publishFixedHeader = (MqttFixedHeader)mqttMessage.fixedHeader();
                MqttPublishVariableHeader publishVariableHeader = (MqttPublishVariableHeader) mqttMessage.variableHeader();
                ByteBuf publishPayload = (ByteBuf) mqttMessage.payload();
                byte[] buf = new byte[publishPayload.readableBytes()];
                publishPayload.readBytes(buf);
                String bufStr = new String(buf);
                String startTag = "<receiverId>";
                String endTag = "</receiverId>";
                int index = bufStr.indexOf(endTag);
                if (index >= 0) {
                    int startIndex = bufStr.indexOf(startTag) + startTag.length();
                    String receiverId = bufStr.substring(startIndex, index);
                    buf = bufStr.substring(index + endTag.length()).getBytes();
                    ((MqttPublishEvent)baseEvent).setReceiverId(receiverId);
                    LOGGER.debug("This publish message is with receiverId {}, fixed payload is {}", receiverId, new String(buf));
                }
                ((MqttPublishEvent)baseEvent).setTopic(publishVariableHeader.topicName());
                ((MqttPublishEvent)baseEvent).setRetain(publishFixedHeader.isRetain());
                ((MqttPublishEvent)baseEvent).setMsgId(publishVariableHeader.messageId());
                ((MqttPublishEvent)baseEvent).setQos(publishFixedHeader.qosLevel().value());
                ((MqttPublishEvent)baseEvent).setMessage(buf);
                break;
            case PUBREC:
                LOGGER.info("Receive PUBREC message");
                MqttMessageIdVariableHeader pubRecVariableHeader = (MqttMessageIdVariableHeader)mqttMessage.variableHeader();
                baseEvent = new MqttPubRecEvent();
                ((MqttPubRecEvent)baseEvent).setMsgId(pubRecVariableHeader.messageId());
                break;
            case PUBREL:
                LOGGER.info("Receive PUBREL message");
                // To get the message ID
                MqttMessageIdVariableHeader pubRelVariableHeader = (MqttMessageIdVariableHeader)mqttMessage.variableHeader();
                baseEvent = new MqttPubRelEvent();
                ((MqttPubRelEvent)baseEvent).setMsgId(pubRelVariableHeader.messageId());
                break;
            case PUBCOMP:
                LOGGER.info("Receive PUBCOMP message");
                // To get the message ID
                MqttMessageIdVariableHeader pubCompVariableHeader = (MqttMessageIdVariableHeader)mqttMessage.variableHeader();
                baseEvent = new MqttPubCompEvent();
                ((MqttPubCompEvent)baseEvent).setMessageId(pubCompVariableHeader.messageId());
                break;
            case PUBACK:
                LOGGER.info("Receive PUBACK message");
                MqttMessageIdVariableHeader pubAckVariableHeader = (MqttMessageIdVariableHeader)mqttMessage.variableHeader();
                baseEvent = new MqttPubAckEvent();
                ((MqttPubAckEvent)baseEvent).setMsgId(pubAckVariableHeader.messageId());
                break;
            case PINGREQ:
                ctx.writeAndFlush(createMessageWithFixedHeader(MqttMessageType.PINGRESP));
                break;
            default:
                break;
        }
        if (baseEvent != null) {
            baseEvent.setMqttMessage(mqttMessage);
            baseEvent.setChannel(channel);
            baseEvent.setClientId(clientId);
            baseEvent.setCleanSession(cleanSession);
            baseEvent.setKeepAlived(keepAlived);
        }
        if (baseEvent != null) {
            mqttEventProcesser.enqueue((MqttEvent) baseEvent);
        }
    }

    private static MqttMessage createMessageWithFixedHeader(MqttMessageType messageType) {
        return new MqttMessage(new MqttFixedHeader(messageType, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        NettyChannel session = channelMapper.get(ctx);
        channelMapper.remove(ctx);
    }

    @Override
    public boolean isSharable() {
        return true;
    }
}
