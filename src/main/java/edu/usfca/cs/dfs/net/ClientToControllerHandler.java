package edu.usfca.cs.dfs.net;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.Client;
import edu.usfca.cs.dfs.StorageMessages;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class ClientToControllerHandler 
extends SimpleChannelInboundHandler<StorageMessages.StorageMessageWrapper> {
	private static Logger logger = LogManager.getLogger();
	
	private Client client;

	public ClientToControllerHandler(Client client) { 
		this.client = client;
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
		
		if (msg.hasStoreResMsg()) {
			// msg from Controller. 
			
			client.getListSNsToStore(msg);
			
    	} else if (msg.hasRetrieveResMsg()) {
			// msg from Controller. 

    		client.getListSNsToRetrieve(msg);
			
		} else if (msg.hasInfoResMsg()) {
			// msg from Controller. 
			
			client.getInfo(msg);
			
		}
		
		
		
		
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		logger.trace("channelReadComplete()" + " ctx: " + ctx);

		if (client.isStoringFile) {
			client.sendStorageReq(client);
		} else {
			client.sendRetrieveReq(client);
		}

		ctx.flush();
		logger.trace("ctx closed. ctx: " + ctx);
		ctx.close();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}
}































