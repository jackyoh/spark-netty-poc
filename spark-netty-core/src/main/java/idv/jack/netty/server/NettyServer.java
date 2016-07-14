package idv.jack.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyServer {
	private NioEventLoopGroup group;
	private Channel channel;
	
	public NettyServer(){
	
	}

	public int start(EchoServerHandler serverHandler) throws Exception{

		ServerChannelInitializer serverChannelInitializer = new ServerChannelInitializer(serverHandler);
		
		this.group = new NioEventLoopGroup(8, NettyServer.newDaemonThreadFactory("RPC-Handler-%d"));
		this.channel = new ServerBootstrap()
							.group(group)
							.channel(NioServerSocketChannel.class)
							.childHandler(serverChannelInitializer)
							.option(ChannelOption.SO_BACKLOG, 1)
							.option(ChannelOption.SO_REUSEADDR, true)
							.childOption(ChannelOption.SO_KEEPALIVE, true)
							.bind(0)
							.sync()
							.channel();
			
		int port = ((InetSocketAddress)channel.localAddress()).getPort();
		return port;
		
	}
	
	public static ThreadFactory newDaemonThreadFactory(final String nameFormat){
		return new ThreadFactory(){
			private final AtomicInteger threadId = new AtomicInteger();
			
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(String.format(nameFormat, threadId.incrementAndGet()));
				t.setDaemon(true);
				return t;
			}
			
		};
	}

	public Channel getChannel() {
		return channel;
	}

	public NioEventLoopGroup getGroup() {
		return group;
	}
	
	
}

class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
	private EchoServerHandler serverHandler;
	
	public ServerChannelInitializer(EchoServerHandler serverHandler){
		this.serverHandler = serverHandler;
	}

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ch.pipeline().addLast(this.serverHandler);
	}
}
