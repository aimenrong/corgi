package corgi.hub.core.mqtt.util;

import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.bean.MqttStoreMessage;
import corgi.hub.core.mqtt.common.MqttConstants;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.*;

/**
 * Created by Terry LIANG on 2017/1/3.
 */
public class MqttUtil {

    private static final ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);

    public static MqttPublishMessage createPublishMessage(String topic, int qos, byte[] message, boolean retained, int msgId) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(qos), retained, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, msgId);
        ByteBuf payload =  ALLOCATOR.buffer();
        payload.writeBytes(message);
        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, payload);
    }

    public static MqttSubAckMessage createSubAckMessage(int msgId, int[] ackType) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(msgId);
        MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(ackType);
        return new MqttSubAckMessage(mqttFixedHeader, mqttMessageIdVariableHeader, mqttSubAckPayload);
    }

    /**
     * Create below message
     * MqttMessageType.PUBACK
     * MqttMessageType.PUBREC
     * MqttMessageType.PUBREL
     * MqttMessageType.PUBCOMP
     * MqttMessageType.UNSUBACK
     * @param messageType
     * @param msgId
     * @return
     */
    public static MqttMessage createGeneralMessage(MqttMessageType messageType, int msgId) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(
                        messageType,
                        false,
                        messageType == MqttMessageType.PUBREL ? MqttQoS.AT_LEAST_ONCE :  MqttQoS.AT_MOST_ONCE,
                        false,
                        0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(msgId);
        return new MqttMessage(mqttFixedHeader, mqttMessageIdVariableHeader);
    }

    public static String formatPublishKey(String clientId, int msgId) {
        return String.format("%s%d", clientId, msgId);
    }

    public static String formatSubscriptionKey(String clientId, String topic) {
        return String.format("%s:%s", clientId, topic);
    }

    public static int mqttClientType(String clientId) {
        if (null == clientId) {
            return -1;
        }
        String[] items = clientId.split(":");
        if (items.length < 3) {
            return -2;
        }
        String type = items[2];
        if (type.startsWith("APPLICATION")) {
            return 0;
        } else if (type.startsWith("DEVICE")) {
            return 1;
        }
        return -3;
    }

    public static String mqttClientTypeString(String clientId) {
        if (null == clientId) {
            return null;
        }
        String[] items = clientId.split("\\" + MqttConstants.CLIENTID_DELIMETER);
        if (items.length < 3) {
            return null;
        }
        String type = items[2];
        if (type.startsWith("APPLICATION")) {
            return MqttConstants.COMMAND_TOPIC;
        } else if (type.startsWith("DEVICE")) {
            return MqttConstants.EVENT_TOPIC;
        }
        return null;
    }

    public static String formatStoreMessageKey(String clientId, int msgId, String topic) {
        return String.format("%s:%d:%s", clientId, msgId, topic);
    }

    public static String formatStoreMessageKey(String clientId, long msgId, String topic) {
        return String.format("%s:%d:%s", clientId, msgId, topic);
    }

    public static MqttStoreMessage convertToMqttStoreMessage(MqttPublishEvent mqttPublishEvent) {
        MqttStoreMessage mqttStoreMessage = new MqttStoreMessage();
        mqttStoreMessage.setClientId(mqttPublishEvent.getClientId());
        mqttStoreMessage.setMsgId(mqttPublishEvent.getMsgId());
        mqttStoreMessage.setTopic(mqttPublishEvent.getTopic());
        mqttStoreMessage.setCreateTime(mqttPublishEvent.getCreateTime());
        mqttStoreMessage.setQos(mqttPublishEvent.getQos());
        mqttStoreMessage.setRetain(mqttPublishEvent.isRetain());
        mqttStoreMessage.setMessage(mqttPublishEvent.getMessage().clone());
        if (mqttPublishEvent.getReceiverId() != null) {
            mqttStoreMessage.setKey(mqttPublishEvent.getReceiverId());
        }
        return mqttStoreMessage;
    }

    public static void publish2Subscribers(ServerChannel subscribeChannel, String clientId, String topic, int qos, byte[] message, boolean retain, long msgId) {
        if (qos == 0) {
            sendPublishMessageToSubscriber(subscribeChannel, clientId, topic, qos, message, retain);
        } else {
            sendPublishMessageToSubscriber(subscribeChannel, clientId, topic, qos, message, retain, msgId);
        }
    }

    public static void sendPublishMessageToSubscriber(ServerChannel subscribeChannel, String clientId, String topic, int qos, byte[] message, boolean retained) {
        sendPublishMessageToSubscriber(subscribeChannel, clientId, topic, qos, message, retained, 0);
    }

    public static void sendPublishMessageToSubscriber(ServerChannel subscribeChannel, String clientId, String topic, int qos, byte[] message, boolean retained, long msgId) {
        MqttPublishMessage pubMessage = MqttUtil.createPublishMessage(topic, qos, message, retained, (int)msgId);
        try {
            subscribeChannel.write(pubMessage);
        }catch(Throwable t) {
            t.printStackTrace();
        }
    }

    public static boolean isBroadcastMessage(String key) {
        if ("*".equals(key)) {
            return true;
        }
        return false;
    }
}
