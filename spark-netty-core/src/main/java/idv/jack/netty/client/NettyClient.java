package idv.jack.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

public class NettyClient {
	private String host;
	private int port;

	public NettyClient(String host, int port){
		this.host = host;
		this.port = port;
	}
	
	public void start(String value) throws Exception{
		EventLoopGroup group = new NioEventLoopGroup();
		
		Bootstrap b = new Bootstrap();
		b.group(group)
		 .channel(NioSocketChannel.class)
		 .remoteAddress(new InetSocketAddress(host, port))
		 .handler(new ClientChannelInitializer(value));
		 ChannelFuture f = b.connect().sync();
		 f.channel().closeFuture().sync();
		 group.shutdownGracefully().sync();
	}
}

class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {
	private String value;
	
	public ClientChannelInitializer(String value){
		this.value = value;
	}
	
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ch.pipeline().addLast(new EchoClientHandler(this.value));
	}
	
}
