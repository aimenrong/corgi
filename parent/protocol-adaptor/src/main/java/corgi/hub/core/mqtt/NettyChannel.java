package corgi.hub.core.mqtt;

import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Terry LIANG on 2016/12/23.
 */
public class NettyChannel implements  ServerChannel {
    private ChannelHandlerContext channel;

    private Map<String, Object> attributes = new HashMap<>();

    NettyChannel(ChannelHandlerContext ctx) {
        channel = ctx;
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public void setIdleTime(int idleTime) {
//        if (channel.pipeline().names().contains("idleStateHandler")) {
//            channel.pipeline().remove("idleStateHandler");
//        }
//        if (channel.pipeline().names().contains("idleEventHandler")) {
//            channel.pipeline().remove("idleEventHandler");
//        }
//        channel.pipeline().addFirst("idleStateHandler", new IdleStateHandler(0, 0, idleTime));
//        channel.pipeline().addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
    }

    public void close(boolean immediately) {
        channel.close();
    }

    public void write(Object value) {
        channel.channel().writeAndFlush(value);
    }

    @Override
    public boolean isActive() {
        return channel.channel().isActive();
    }
}
