package corgi.hub.core.mqtt.common;

import corgi.hub.core.mqtt.util.MqttUtil;
import corgi.hub.core.mqtt.event.MqttPublishEvent;
import corgi.hub.core.mqtt.ServerChannel;
import corgi.hub.core.mqtt.event.MqttPubRelEvent;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;

/**
 * Created by Terry LIANG on 2017/1/7.
 */
public class ProducerQos2Callback implements IKafkaCallback<MqttPubRelEvent> {
    @Override
    public void callback(HubContext context, MqttPubRelEvent mqttPubRelEvent) {
        String clientId = mqttPubRelEvent.getClientId();
        int msgId = mqttPubRelEvent.getMsgId();
        String publishKey = MqttUtil.formatPublishKey(clientId, msgId);
        MqttPublishEvent publishEvent = (MqttPublishEvent) context.getSubscriptionManager().getStorageService().retrievePublishedMessage(publishKey);
        context.getSubscriptionManager().getStorageService().removePublishedMessage(publishKey);
        sendPubComp(context.getSession(clientId).getChannel(), clientId, msgId);
    }

    private void sendPubComp(ServerChannel channel, String clientId, int msgId) {
        MqttMessage pubCompMsg = MqttUtil.createGeneralMessage(MqttMessageType.PUBCOMP, msgId);
        channel.write(pubCompMsg);
    }
}
