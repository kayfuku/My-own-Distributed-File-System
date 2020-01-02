package edu.usfca.cs.dfs.net;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageNode;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

@ChannelHandler.Sharable
public class StorageNodeToControllerHandler 
extends SimpleChannelInboundHandler<StorageMessages.StorageMessageWrapper> {
	private static Logger logger = LogManager.getLogger();
	
	private StorageNode storageNode;

	public StorageNodeToControllerHandler(StorageNode storageNode) { 
		this.storageNode = storageNode;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) {
		/* A connection has been established */
		InetSocketAddress addr
		= (InetSocketAddress) ctx.channel().remoteAddress();
		logger.trace("Connection established: " + addr + " ctx: " + ctx);
		
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws IOException {
		/* A channel has been disconnected */
		InetSocketAddress addr
		= (InetSocketAddress) ctx.channel().remoteAddress();
		logger.trace("Connection lost: " + addr + " ctx: " + ctx);
		
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx)
			throws Exception {
		/* Writable status of the channel changed */
	}

	@Override
	public void channelRead0(
			ChannelHandlerContext ctx,
			StorageMessages.StorageMessageWrapper msg) throws IOException  {
		logger.trace("channelRead0() start. ctx: " + ctx);
		
		

	}
	
	
	// For heartbeat. 
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
			throws Exception {

		if (evt instanceof IdleStateEvent) {
			
			storageNode.sendHeartbeat(ctx, evt);
 
		} else {
			super.userEventTriggered(ctx, evt);
		}
	}


	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		logger.trace("channelReadComplete()" + " ctx: " + ctx);

		ctx.flush();
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}
}































