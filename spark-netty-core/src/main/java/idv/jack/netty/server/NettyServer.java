package idv.jack.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

public class NettyServer {
	private int port;
	
	public NettyServer(int port){
		this.port = port;
	}
	
	/*public static void main(String args[]) throws Exception{
		int port = 1234;
		if(args.length >= 1){
			port = Integer.parseInt(args[0]);
		}
		new NettyServer(port).start(new EchoServerHandler());
	}*/

	public void start(EchoServerHandler serverHandler) throws Exception{
		NioEventLoopGroup group = new NioEventLoopGroup();
		ServerBootstrap b = new ServerBootstrap();
		ServerChannelInitializer serverChannelInitializer = new ServerChannelInitializer(serverHandler);
		
		b.group(group)
		 .channel(NioServerSocketChannel.class)
		 .localAddress(new InetSocketAddress(port))
		 .childHandler(serverChannelInitializer);
		
		ChannelFuture f = b.bind().sync();
		System.out.println(NettyServer.class.getName() + " started and listen on " + f.channel().localAddress());
		f.channel().closeFuture().sync();
		group.shutdownGracefully().sync();
		
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
