package idv.jack.netty.server;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

public class EchoServerHandler extends ChannelInboundHandlerAdapter{
	private List<String> resultList = new ArrayList<String>();

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg){
		ByteBuf in = (ByteBuf)msg;
		//String value = "aaaaabbbbdasdfadasdfasd";
		//in.writeBytes(value.getBytes());
		//System.out.println("Server received: " + in.toString(CharsetUtil.UTF_8));
		this.resultList.add(in.toString(CharsetUtil.UTF_8));
		ctx.write(in);
	}
	
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception{
		ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
		   .addListener(ChannelFutureListener.CLOSE);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx,
			                      Throwable cause){
		cause.printStackTrace();
		ctx.close();
	}

	public List<String> getResultList() {
		return resultList;
	}

	public void setResultList(List<String> resultList) {
		this.resultList = resultList;
	}
	
}
