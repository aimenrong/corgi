/**
 * 
 */
package corgi.hub.core.netty;

import corgi.hub.core.ssl.SslContextFactory;
import io.netty.channel.*;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLEngine;

/**
 * @author a
 *
 */
@Component("nettyServerBookstrap")
public class NettyServerBootstrap {

	@Autowired
	private ChannelInboundHandlerAdapter mqttServerHandler;

//    @Value("${app.long.conn.port:2883}")
	private int port = 2883;

	private int sslPort = 8883;

	private SocketChannel socketChannel;

	public NettyServerBootstrap() {
        System.out.println(String.format("%s init success", this.getClass().getName()));
	}

	@PostConstruct
	public void bind() throws InterruptedException {
		EventLoopGroup boss = new NioEventLoopGroup();
		EventLoopGroup worker = new NioEventLoopGroup();
		ServerBootstrap bootstrap = new ServerBootstrap();
		bootstrap.group(boss, worker);
		bootstrap.channel(NioServerSocketChannel.class);
		bootstrap.option(ChannelOption.SO_BACKLOG, 128);
		bootstrap.option(ChannelOption.TCP_NODELAY, true);
		// keep long connection
		bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel socketChannel) throws Exception {
				ChannelPipeline p = socketChannel.pipeline();
                p.addLast("encoder", MqttEncoder.INSTANCE);
				p.addLast("decoder", new MqttDecoder());
				p.addLast(mqttServerHandler);
				
			}
		});
		ChannelFuture f = bootstrap.bind(port).sync();
		if (f.isSuccess()) {
			System.out.println("mqtt server start---------------");
		}
	}

	public void bindWithSsl() throws InterruptedException {
		EventLoopGroup boss = new NioEventLoopGroup();
		EventLoopGroup worker = new NioEventLoopGroup();
		ServerBootstrap bootstrap = new ServerBootstrap();
		bootstrap.group(boss, worker);
		bootstrap.channel(NioServerSocketChannel.class);
		bootstrap.option(ChannelOption.SO_BACKLOG, 128);
		bootstrap.option(ChannelOption.TCP_NODELAY, true);
		// keep long connection
		bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel socketChannel) throws Exception {
				ChannelPipeline p = socketChannel.pipeline();
				// SSL
				SSLEngine engine = SslContextFactory.getServerContext().createSSLEngine();
				engine.setUseClientMode(false);
				engine.setNeedClientAuth(true);
				p.addLast("ssl", new SslHandler(engine));
				// On top of the SSL handler, add the text line codec.
//				p.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));

				// MQTT
				p.addLast("encoder", MqttEncoder.INSTANCE);
				p.addLast("decoder", new MqttDecoder());
				p.addLast(mqttServerHandler);
			}
		});
		ChannelFuture f = bootstrap.bind(sslPort).sync();
		if (f.isSuccess()) {
			System.out.println("mqtt ssl server start---------------");
		}
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}

	public void setSocketChannel(SocketChannel socketChannel) {
		this.socketChannel = socketChannel;
	}
}
