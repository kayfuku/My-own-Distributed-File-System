package edu.usfca.cs.dfs.net;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class InboundHandler 
extends SimpleChannelInboundHandler<StorageMessages.StorageMessageWrapper> {
	private static Logger logger = LogManager.getLogger();
	
	private BufferedOutputStream bos;
	private static String outputPath;


    public InboundHandler(String outputPath) { 
    	this.outputPath = outputPath;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        logger.trace("Connection established: " + addr);
        
		
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws IOException {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        logger.trace("Connection lost: " + addr);
        
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
    	logger.trace("channelRead0() start. ");
    	
    	if (msg.hasStoreReqMsg()) {
    		StorageMessages.StoreReq storeReqMsg = msg.getStoreReqMsg();
    		String storeReqFileName = storeReqMsg.getFileName();
    		
    		// Create a response. 
//    		String resStr = "hello, client! I got a file " + storeReqFileName;
//    		logger.trace(resStr);
//    		StorageMessages.StoreRes storeResMsg = StorageMessages.StoreRes.newBuilder()
//    				.setResponse(resStr)
//    				.build();
//    		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
//    				.setStoreResMsg(storeResMsg)
//    				.build();			
    		
    		// TODO: Send it back. ???
//    		ctx.writeAndFlush(msgWrapper);
//    		ctx.channel().writeAndFlush(msgWrapper);
//    		ctx.channel().write(msgWrapper);
//    		final ChannelFuture f = ctx.channel().writeAndFlush(msgWrapper);
//    	    f.addListener(ChannelFutureListener.CLOSE);
//    		f.syncUninterruptibly();
    		
    		logger.trace("after sending it back.");
    		
    	} else if (msg.hasStoreResMsg()) {
    		logger.trace("got response!");
    		
//    		StorageMessages.StoreRes storeResMsg = msg.getStoreResMsg();
//    		String resStr = storeResMsg.getResponse();
//    		logger.trace(resStr);
    		
    		
    		
    		
			
		} else if (msg.hasStoreChunkReqMsg()) {
    		StorageMessages.StoreChunkReq storeChunkMsg = msg.getStoreChunkReqMsg();
    		logger.trace(storeChunkMsg);

            String chunkFileName = storeChunkMsg.getFileName();
            ByteString data = storeChunkMsg.getData();
            logger.trace("Writing chunk file name: " + chunkFileName);
            
            File chunkFile = new File(outputPath + chunkFileName);
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(chunkFile));
            
            // Write the data to the disk. 
    		bos.write(data.toByteArray());
            
    		bos.close();
		}
    	
        
    }
    
    
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    	logger.trace("server channelReadComplete..");
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
    
    

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}































