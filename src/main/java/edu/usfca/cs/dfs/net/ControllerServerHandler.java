package edu.usfca.cs.dfs.net;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.Controller;
import edu.usfca.cs.dfs.StorageMessages;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class ControllerServerHandler 
extends SimpleChannelInboundHandler<StorageMessages.StorageMessageWrapper> {
	private static Logger logger = LogManager.getLogger();
	
	private Controller controller;


    public ControllerServerHandler(Controller controller) { 
    	this.controller = controller;
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
		
//        String ipAddr = addr.getAddress().toString().substring(1);
//		controller.detectFailure(ipAddr);
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
    	
    	logger.trace("channelRead0() start.");  
    	
    	if (msg.hasStorageAddrMsg()) {
    		// msg from SN. 
    		
    		controller.getStorageAddress(ctx, msg);
    		
		} else if (msg.hasHeartbeatMsg()) {
    		// msg from SN. 
			
			controller.getHeartbeat(ctx, msg);
			
		} else if (msg.hasStoreReqMsg()) {
    		// msg from Client. 
			
			controller.getStorageReq(ctx, msg);
    		
    	} else if (msg.hasRetrieveReqMsg()) {
			// msg from Client. 
    		
    		controller.getRetrieveReq(ctx, msg);
    		
		} else if (msg.hasInfoReqMsg()) {
			// msg from Client. 

			controller.getInfo(ctx, msg);
		
		}
    	
    	
    }
    
	@Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		logger.trace("channelReadComplete()" + " ctx: " + ctx);
        ctx.flush();
//        ctx.close();
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		logger.trace("exceptionCaught()" + " ctx: " + ctx);

        cause.printStackTrace();
        ctx.close();
    }
    
    
    
    
    
    
    
    
}































