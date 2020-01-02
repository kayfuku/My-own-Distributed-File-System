package edu.usfca.cs.dfs;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import edu.usfca.cs.dfs.net.StoragePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

public class StorageNode {
	private static Logger logger = LogManager.getLogger();

	private static String CONTROLLER_IP_ADDRESS = "localhost"; // local
//	private static String CONTROLLER_IP_ADDRESS = "10.0.1.21"; // orion01
	private static int CONTROLLER_PORT = 7000;

	private static ServerMessageRouter messageRouter;
	private static int port;
	private static String addrSn;
	private static float spaceAvailable = 0;
	private static int numRequests = 0;
    private static String OUTPUT_PATH_BASE = "bigdata/kfukutani/"; // local
//	private static String OUTPUT_PATH_BASE = "/bigdata/kfukutani/"; // orion

	private static final int READ_IDEL_TIME_OUT = 0; // disable
	private static final int WRITE_IDEL_TIME_OUT = 0; // disable
	private static final int ALL_IDEL_TIME_OUT = 5;


	public StorageNode() {

	}
	public int getReadIdleTimeout() {
		return READ_IDEL_TIME_OUT;
	}
	public int getWriteIdleTimeout() {
		return WRITE_IDEL_TIME_OUT;
	}
	public int getAllIdleTimeout() {
		return ALL_IDEL_TIME_OUT;
	}

	public void start(StorageNode storageNode) throws IOException {
		messageRouter = new ServerMessageRouter(null, storageNode);
		messageRouter.listen(port);
		logger.trace(addrSn + ", Listening for connections on port " + port);
		System.out.println(addrSn + ", Listening for connections on port " + port);
	}

	private void startup(StorageNode storageNode) throws IOException {
		logger.trace("startup() start.");

		EventLoopGroup workerGroup = new NioEventLoopGroup();
		StoragePipeline pipeline = new StoragePipeline(storageNode);

		Bootstrap bootstrap = new Bootstrap()
				.group(workerGroup)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true)
				.handler(pipeline);

		// Connect to Controller.  
		ChannelFuture channelFutureToController = bootstrap.connect(CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
		channelFutureToController.syncUninterruptibly();


		sendAddress(channelFutureToController);



		// Delete old files. 



	}


	private static void sendAddress(ChannelFuture cf) {    	
		Channel chan = cf.channel();

		File file = new File(OUTPUT_PATH_BASE);
		spaceAvailable = file.getUsableSpace() / 1000000000;

		StorageMessages.StorageAddr storageAddrMsg = StorageMessages.StorageAddr.newBuilder()
				.setStorageAddr(addrSn)
				.setSpace(spaceAvailable)
				.setNumReq(numRequests)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setStorageAddrMsg(storageAddrMsg)
				.build();			

		logger.trace(addrSn + " Sending address..");
		ChannelFuture channelFuture = chan.writeAndFlush(msgWrapper);
		channelFuture.syncUninterruptibly();
	}


	public void getChunks(ChannelHandlerContext ctx, StorageMessageWrapper msg) throws IOException {
		numRequests++;

		// SN receives a chunk from Client. 
		StorageMessages.StoreChunkReq storeChunkReqMsg = msg.getStoreChunkReqMsg();
		logger.trace(addrSn + " storeChunkMsg: " + storeChunkReqMsg);
		String inputFileName = storeChunkReqMsg.getFileName();
		int chunkId = storeChunkReqMsg.getChunkId();
		byte[] data = storeChunkReqMsg.getData().toByteArray();

		// Make directory for this file chunks. 
		String fileDirPath = OUTPUT_PATH_BASE + inputFileName + "_" + addrSn;
		File outputPathDir = new File(fileDirPath);
		outputPathDir.mkdir();
		String chunkFileName = inputFileName + "_chunk" + chunkId;

		// Calculate the entropy of the file to see if it should be compressed. 
		double entropy = entropy(data);
		logger.trace(addrSn + " entropy: " + entropy);
		double maxComp = (1 - (entropy / 8));
		logger.trace(addrSn + " max compression: " + maxComp);
		BufferedOutputStream bos = null;
		if (maxComp > 0.6) {
			// The file is efficient to be compressed. 

			// Change chunk file name. 
			chunkFileName = chunkFileName + "_comp";
			// Compress. 
			ByteArrayOutputStream compressBaos = new ByteArrayOutputStream();
			try (OutputStream gzip = new GZIPOutputStream(compressBaos)) {
				gzip.write(data);
			}
			byte[] compressed = compressBaos.toByteArray();
			//			LZ4Factory factory = LZ4Factory.fastestInstance();
			//			LZ4Compressor compressor = factory.fastCompressor();
			//			byte[] compressed = compressor.compress(data);

			// Generate checksum. 
			String checksum = checksum(compressed);
			logger.trace(addrSn + " storing checksum: " + checksum);
			chunkFileName = chunkFileName + "_checksum_" + checksum;

			// Write the compressed data to the disk. 
			logger.debug(addrSn + " writing the compressed chunk file: " + chunkFileName);
			File chunkFile = new File(fileDirPath + "/" + chunkFileName);  
			bos = new BufferedOutputStream(new FileOutputStream(chunkFile));
			bos.write(compressed);

		} else {
			// The file is not efficient to be compressed. 

			String checksum = checksum(data);
			logger.trace(addrSn + " storing checksum: " + checksum);
			chunkFileName = chunkFileName + "_checksum_" + checksum;

			// Write the data to the disk. 
			logger.debug(addrSn + " writing chunk file: " + chunkFileName);
			File chunkFile = new File(fileDirPath + "/" + chunkFileName);  
			bos = new BufferedOutputStream(new FileOutputStream(chunkFile));
			bos.write(data);
		}

		bos.close();

		// Reply to notify Client that storing is completed. 
		// Create a response. 
		boolean storingDone = true;
		StorageMessages.StoreDone storeDoneMsg = StorageMessages.StoreDone.newBuilder()
				.setIsDone(storingDone)
				.setChunkId(chunkId)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setStoreDoneMsg(storeDoneMsg)
				.build();			

		// Send it back. 
		logger.trace(addrSn + " Storing done, sending Done message...");
		ctx.channel().writeAndFlush(msgWrapper);
		ctx.writeAndFlush(msg);
	}


	/**
	 * Calculates the entropy per character/byte of a byte array.
	 * @param input  array to calculate entropy of
	 * @return entropy  bits per byte
	 */
	public static double entropy(byte[] input) {
		if (input.length == 0) {
			return 0.0;
		}

		/* Total up the occurrences of each byte */
		int[] charCounts = new int[256];
		for (byte b : input) {
			charCounts[b & 0xFF]++;
		}

		double entropy = 0.0;
		for (int i = 0; i < 256; ++i) {
			if (charCounts[i] == 0.0) {
				continue;
			}

			double freq = (double) charCounts[i] / input.length;
			entropy -= freq * (Math.log(freq) / Math.log(2));
		}

		return entropy;
	}


	/**
	 * Generate checksum of the input file. 
	 * @param filepath  file to calculate checksum of
	 * @return hex string 
	 * @throws IOException
	 */
	private static String checksum(byte[] bytes) throws IOException {

		// Generate checksum of the file. 
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
			byte[] buffer = new byte[1024];
			int nread;
			while ((nread = bais.read(buffer)) != -1) {
				md.update(buffer, 0, nread);
			}
		}

		// Convert bytes to hex. 
		StringBuilder sb = new StringBuilder();
		for (byte b : md.digest()) {
			sb.append(String.format("%02x", b));
		}

		return sb.toString();
	}


	public void sendHeartbeat(ChannelHandlerContext ctx, Object evt) {
		IdleStateEvent event = (IdleStateEvent) evt;
		String type = "";
		if (event.state() == IdleState.READER_IDLE) {
			type = "read idle";
		} else if (event.state() == IdleState.WRITER_IDLE) {
			type = "write idle";
		} else if (event.state() == IdleState.ALL_IDLE) {
			type = "all idle";
		}

		// Collect info. 
		File file = new File(OUTPUT_PATH_BASE);
		spaceAvailable = file.getUsableSpace() / 1000000000;

		// Create a heartbeat message. 
		String heartbeatStr = "Heartbeat! (" + type + ")";
		StorageMessages.Heartbeat heartbeatMsg = StorageMessages.Heartbeat.newBuilder()
				.setHeartbeat(heartbeatStr)
				.setSpace(spaceAvailable)
				.setNumReq(numRequests)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setHeartbeatMsg(heartbeatMsg)
				.build();

		// Send a heartbeat. 
		ctx.writeAndFlush(msgWrapper).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

		logger.trace(addrSn + ", Send " + heartbeatStr);

	}


	public void getFileName(ChannelHandlerContext ctx, StorageMessageWrapper msg) throws IOException {
		numRequests++;

		// SN receives file name from Client. 
		StorageMessages.RetrieveReq retrieveReqMsg = msg.getRetrieveReqMsg();
		logger.trace(addrSn + " retrieveFileMsg: " + retrieveReqMsg);
		String fileName = retrieveReqMsg.getFileName();

		// Find and read chunk files. 
		File fileDir = new File(OUTPUT_PATH_BASE + fileName + "_" + addrSn);
		File[] files = fileDir.listFiles();

		for (File file : files) {
			String chunkFileName = file.getName();
			// For Bloom Filter false positive cases. 
			if (!chunkFileName.startsWith(fileName)) {
				continue;
			}
			logger.trace(addrSn + " chunkFileName: " + chunkFileName);

			// Check checksum. 
			byte[] data = Files.readAllBytes(file.toPath());
			String checksum = checksum(data);
			logger.trace(addrSn + " checksum: " + checksum);
			String checksumOriginal = chunkFileName.substring(chunkFileName.indexOf("_checksum") + 10);

			//			String checksumOriginal = fileNameToChecksum.get(chunkFileName);
			logger.trace(addrSn + " checksumOriginal: " + checksumOriginal);
			if (!checksum.equals(checksumOriginal)) {
				// File has been corrupted.
				logger.trace(addrSn + " File corrupted!");
				// TODO: 

				continue; // local 

			}
			logger.trace(addrSn + " Checksum matched!");

			chunkFileName = chunkFileName.substring(0, chunkFileName.indexOf("_checksum"));

			// Decompression if needed. 
			byte[] dataToSendBack = null;
			if (chunkFileName.contains("comp")) {
				// Decompress. 
				ByteArrayOutputStream decompressBaos = new ByteArrayOutputStream();
				try (InputStream gzip = new GZIPInputStream(new ByteArrayInputStream(data))) {
					int b;
					while ((b = gzip.read()) != -1) {
						decompressBaos.write(b);
					}
				}
				byte[] decompressed = decompressBaos.toByteArray();
				dataToSendBack = Arrays.copyOf(decompressed, decompressed.length);

				//				LZ4Factory factory = LZ4Factory.fastestInstance();
				//		        LZ4FastDecompressor decompressor = factory.fastDecompressor();
				//				byte[] compressed = decompressor.decompress(data, sizeBeforeCompression);

				chunkFileName = chunkFileName.substring(0, chunkFileName.indexOf("_comp"));

			} else {

				dataToSendBack = data;
			}

			// Get chunk id. 
			String chunkStr = chunkFileName.substring(chunkFileName.indexOf("_chunk"));
			int chunkId = Integer.parseInt(chunkStr.substring(6));

			// Create a response. 
			fileName = chunkFileName.substring(0, chunkFileName.indexOf("_chunk"));
			ByteString dataByteStr = ByteString.copyFrom(dataToSendBack);
			StorageMessages.RetrieveChunkRes retrieveChunkResMsg = StorageMessages.RetrieveChunkRes.newBuilder()
					.setFileName(fileName)
					.setChunkId(chunkId)
					.setData(dataByteStr)
					.build();
			StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
					.setRetrieveChunkResMsg(retrieveChunkResMsg)
					.build();			

			// Send it back to Client. 
			logger.trace(addrSn + " Sending chunk back to Client. chunkId: " + chunkId);
			ctx.channel().writeAndFlush(msgWrapper);
		}

		ctx.close();
	}




	public static void main(String[] args) throws IOException {
		port = Integer.parseInt(args[0]);
		InetAddress addr = InetAddress.getLocalHost();
		String ipAddr = addr.getHostAddress();
		addrSn = ipAddr + ":" + port;
		logger.trace("address: " + addrSn);

		StorageNode s = new StorageNode();
		s.start(s);

		s.startup(s);

	}



}































































