package edu.usfca.cs.dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.ClientPipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {
	private static Logger logger = LogManager.getLogger();

	private static Client client;
	//	private static boolean running = true;
	private static String INPUT_FILE_PATH = "data/input/";
//	private static String INPUT_FILE_PATH = "/bigdata/mmalensek/p1/";
	private static String inputFileName;
	private static String OUTPUT_FILE_PATH = "data/output/";
	private static String outputFileName;

	private static String CONTROLLER_IP_ADDRESS = "localhost"; // local
//	private static String CONTROLLER_IP_ADDRESS = "10.0.1.21"; // orion01
	private static int CONTROLLER_PORT = 7000;

	private static int CHUNK_SIZE = 5; 
	public List<String> listStorageNodes;
	public boolean isStoringFile;
	private int totalNumChunks;
	private int numChunks;
	private TreeMap<Integer, byte[]> chunkIdToData;

	public Client() {
		listStorageNodes = new ArrayList<>();
		isStoringFile = false;
		numChunks = 0;
		chunkIdToData = new TreeMap<>();

	}

	private static ChannelFuture setupNettyClient(Client client, String destination, String ipAddr, int port) {
		logger.trace("setupNettyClient() start. ");

		EventLoopGroup workerGroup = new NioEventLoopGroup();
		ClientPipeline pipeline = new ClientPipeline(client, destination);

		Bootstrap bootstrap = new Bootstrap()
				.group(workerGroup)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true)
				.handler(pipeline);

		logger.debug("Connecting to " + ipAddr + ":" + port + "...");
		ChannelFuture channelFuture = bootstrap.connect(ipAddr, port);
		channelFuture.syncUninterruptibly();

		return channelFuture;
	}


	private void requestListOfSNsToStore(ChannelFuture cf) {
		logger.trace("requestListOfSNsToStore() start. ");
		Channel chan = cf.channel();

		// Get number of chunks. 
		File inputFile = new File(INPUT_FILE_PATH + inputFileName);
		totalNumChunks = (int) Math.ceil((double) inputFile.length() / CHUNK_SIZE);
		numChunks = totalNumChunks;
		logger.trace("numChunks: " + numChunks);

		// Create a store request message. 
		StorageMessages.StoreReq storeReqMsg = StorageMessages.StoreReq.newBuilder()
				.setFileName(inputFileName)
				.setNumChunks(numChunks)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setStoreReqMsg(storeReqMsg)
				.build();

		// Send the message. 
		isStoringFile = true;
		ChannelFuture channelFuture = chan.writeAndFlush(msgWrapper);
		channelFuture.syncUninterruptibly();				
	}


	public void getListSNsToStore(StorageMessageWrapper msg) {
		// Client receives a list of SNs from Controller. 
		logger.trace("[Client] Got response!");

		StorageMessages.StoreRes storeResMsg = msg.getStoreResMsg();
		listStorageNodes = storeResMsg.getAddressesList();

		logger.debug("listStorageNodes: " + listStorageNodes);
	}


	// Client to Storage Node (SN). 
	// This method is called from ClientToControllerHandler.channelReadComplete(). 
	public void sendStorageReq(Client client) throws IOException {
		logger.trace("sendStorageReq() start.");

		// Read a file and put it into byte array. 
		File myFile = new File(INPUT_FILE_PATH + inputFileName);
		byte[] buffer = new byte[CHUNK_SIZE];
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(myFile));

		// Setup connection to each SN. 
		List<ChannelFuture> writes = new ArrayList<>();
		for (String addrSN : listStorageNodes) {
			logger.debug("Setup connection to SN " + addrSN);
			String ipAddrSN = addrSN.split(":")[0];
			int portSN = Integer.parseInt(addrSN.split(":")[1]);
			ChannelFuture channelFutureToSN = setupNettyClient(client, "storageNode", ipAddrSN, portSN);

			writes.add(channelFutureToSN);
		}

		System.out.println("Sending data...");
		int numNodes = listStorageNodes.size();
		int readSize;
		int i = 0;
		while ((readSize = bis.read(buffer)) > 0) {
			// Need to create new byte array! Otherwise, output file will be broken. 
			byte[] copyBuffer = Arrays.copyOf(buffer, readSize);
			//        	logger.trace("send: " + Arrays.toString(copyBuffer));

			ByteString data = ByteString.copyFrom(copyBuffer);
			StorageMessages.StoreChunkReq storeChunkReqMsg = StorageMessages.StoreChunkReq.newBuilder()
					.setFileName(inputFileName)
					.setChunkId(i)
					.setData(data)
					.build();
			StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
					.setStoreChunkReqMsg(storeChunkReqMsg)
					.build();		

			Channel chan = writes.get(i % numNodes).channel();
			logger.debug("Sending chunked data! " + "chunk " + i + "  " + chan);
			chan.writeAndFlush(msgWrapper).syncUninterruptibly();	

			i++;
		}

		bis.close();
		logger.trace("sendStorageReq() end.");
	}


	public void checkFinish(StorageMessageWrapper msg) {
		StorageMessages.StoreDone storeDoneMsg = msg.getStoreDoneMsg();
		int chunkId = storeDoneMsg.getChunkId();
		System.out.println("chunkId: " + chunkId + " /" + totalNumChunks);
		logger.trace("chunkId: " + chunkId + " /" + totalNumChunks);
		numChunks--;
		logger.debug("checkFinish() chunks remaining: " + numChunks);
		if (numChunks == 0) {			
			System.out.println("Storing done.");
			logger.trace("Storing done.");
			System.exit(0);
		}
	}


	private void requestListOfSNsToRetrieve(ChannelFuture cf) {
		logger.trace("requestListOfSNsToRetrieve() start. ");
		Channel chan = cf.channel();

		// Create a store request message. 
		StorageMessages.RetrieveReq retrieveReqMsg = StorageMessages.RetrieveReq.newBuilder()
				.setFileName(outputFileName)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setRetrieveReqMsg(retrieveReqMsg)
				.build();		

		// Send the message. 
		isStoringFile = false;
		ChannelFuture channelFuture = chan.writeAndFlush(msgWrapper);
		channelFuture.syncUninterruptibly();
	}


	public void getListSNsToRetrieve(StorageMessageWrapper msg) {
		// Client receives a list of SNs from Controller. 
		logger.trace("[Client] Got response!");

		StorageMessages.RetrieveRes retrieveResMsg = msg.getRetrieveResMsg();
		listStorageNodes = retrieveResMsg.getAddressesList();
		totalNumChunks = retrieveResMsg.getNumChunks();
		numChunks = totalNumChunks;
		logger.debug("numChunks: " + numChunks);

		logger.debug("listStorageNodes: " + listStorageNodes);
	}


	// Client to Storage Node (SN). 
	// This method is called from ClientToControllerHandler.channelReadComplete(). 
	public void sendRetrieveReq(Client client) {
		// Setup connection to each SN. 

		// Send file name to SNs. 
		for (String addrSN : listStorageNodes) {
			logger.debug("Setup connection to SN " + addrSN);
			String ipAddrSN = addrSN.split(":")[0];
			int portSN = Integer.parseInt(addrSN.split(":")[1]);
			ChannelFuture channelFutureToSN = setupNettyClient(client, "storageNode", ipAddrSN, portSN);

			StorageMessages.RetrieveReq retrieveReqMsg = StorageMessages.RetrieveReq.newBuilder()
					.setFileName(outputFileName)
					.build();
			StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
					.setRetrieveReqMsg(retrieveReqMsg)
					.build();		

			Channel chan = channelFutureToSN.channel();
			logger.debug("Sending retrieval request! " + chan + " " + outputFileName);
			chan.writeAndFlush(msgWrapper).syncUninterruptibly();
		}

	}


	public void reconstructChunks(StorageMessageWrapper msg) throws IOException {

		StorageMessages.RetrieveChunkRes retrieveChunkResMsg = msg.getRetrieveChunkResMsg();
		String inputFileName = retrieveChunkResMsg.getFileName();
		int chunkId = retrieveChunkResMsg.getChunkId();
		logger.trace("inputFileName: " + inputFileName + " chunkId: " + chunkId);
		byte[] data = retrieveChunkResMsg.getData().toByteArray();
		chunkIdToData.put(chunkId, data);

		System.out.println("chunkId: " + chunkId + " /" + totalNumChunks);
		logger.trace("chunkId: " + chunkId + " /" + totalNumChunks);
		numChunks--;
		logger.debug("reconstructChunks() chunks remaining: " + numChunks);
		if (numChunks == 0) {
			File outputFile = new File(OUTPUT_FILE_PATH + "/" + outputFileName);  
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile));

			//			StringBuilder sBuilder = new StringBuilder();
			for (byte[] bytes : chunkIdToData.values()) {
				logger.trace("bytes: " + new String(bytes));
				bos.write(bytes);
				//				sBuilder.append(new String(bytes));
			}

			//			logger.trace("sBuilder: \n" + sBuilder.toString());
			bos.flush();
			bos.close();
			File nullFile = new File(OUTPUT_FILE_PATH + "/null");
			nullFile.delete();
			System.out.println("Retrieving done.");
			logger.trace("Retrieving done.");
			System.exit(0);
		}

	}


	private void reqInfo(ChannelFuture cf) {
		logger.trace("printInfo() start. ");
		Channel chan = cf.channel();

		// Create a store request message. 
		StorageMessages.InfoReq infoReqMsg = StorageMessages.InfoReq.newBuilder()
				.setInfoReq("info")
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setInfoReqMsg(infoReqMsg)
				.build();

		// Send the message. 
		ChannelFuture channelFuture = chan.writeAndFlush(msgWrapper);
		channelFuture.syncUninterruptibly();

	}


	public void getInfo(StorageMessageWrapper msg) {
		// Client receives info from Controller. 
		logger.trace("Got info response!");

		StorageMessages.InfoRes infoResMsg = msg.getInfoResMsg();
		listStorageNodes = infoResMsg.getAddressesList();
		float space = infoResMsg.getSpace();
		Map<String, Integer> snToNumRequests = infoResMsg.getSnToNumRequestsMap();

		System.out.println("Active Nodes: " + listStorageNodes);
		System.out.println("Space Available (GB): " + space);
		System.out.println("Number of Requests: ");
		for (Map.Entry<String, Integer> entry : snToNumRequests.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}

		System.exit(0);
	}


	private static void printHelp() {
		System.out.println("usage: <command> <file name>");
		System.out.println("example: store my_file.txt");
		System.out.println("command: ");
		System.out.println("\t store <file name>: store a file.");
		System.out.println("\t retrieve <file name>: retrieve a file.");		
		System.out.println("\t exit: exit.");
		System.out.println("Bye.");
		System.exit(0);
	}


	public static void main(String[] args) throws IOException {
		Client c = new Client();
		client = c;

		if (args.length < 1) {
			printHelp();
		} 

		String command = args[0];		
		if (command.equals("exit")) {
			System.out.println("Bye.");
			System.exit(0);
		}

		switch (command) {
		case "store":
			logger.trace("store command executed!");
			if (args.length != 2) {
				printHelp();
			}
			inputFileName = args[1];

			// Set up Netty. 
			ChannelFuture cf = setupNettyClient(
					client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
			client.requestListOfSNsToStore(cf);

			//				System.out.println(inputFileName + " saved!");
			logger.trace("store command end.");
			break;

		case "retrieve":
			logger.trace("retrieve command executed!");
			if (args.length != 2) {
				printHelp();
			}
			outputFileName = args[1];

			// Set up Netty. 
			ChannelFuture cf2 = setupNettyClient(
					client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
			client.requestListOfSNsToRetrieve(cf2);

			//				System.out.println(outputFileName + " retrieved!");
			logger.trace("retrieve command end.");
			break;

		case "info":

			// Set up Netty. 
			ChannelFuture cf3 = setupNettyClient(
					client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
			client.reqInfo(cf3);

			logger.trace("info command end.");
			break;

		default:
			printHelp();
			break;
		}







		//		Scanner in = new Scanner(System.in);
		//		while (running) {
		//			System.out.println("Enter a command: ");
		//			String inputLine = in.nextLine();
		//			if (inputLine.equals("exit")) {
		//				System.out.println("Bye.");
		//				break;
		//			}
		//			String[] line = inputLine.split(" ");
		//			if (line.length != 2 && !inputLine.equals("info")) {
		//				printHelp();
		//				continue;
		//			}
		//
		//			String command = line[0];
		//			switch (command) {
		//			case "store":
		//				logger.trace("store command executed!");
		//				inputFileName = line[1];
		//
		//				// Set up Netty. 
		//				ChannelFuture cf = setupNettyClient(
		//						client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
		//				client.requestListOfSNsToStore(cf);
		//
		//				//				System.out.println(inputFileName + " saved!");
		//				logger.trace("store command end.");
		//				break;
		//
		//			case "retrieve":
		//				logger.trace("retrieve command executed!");
		//				outputFileName = line[1];
		//
		//				// Set up Netty. 
		//				ChannelFuture cf2 = setupNettyClient(
		//						client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
		//				client.requestListOfSNsToRetrieve(cf2);
		//
		//				//				System.out.println(outputFileName + " retrieved!");
		//				logger.trace("retrieve command end.");
		//				break;
		//
		//			case "info":
		//
		//				// Set up Netty. 
		//				ChannelFuture cf3 = setupNettyClient(
		//						client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
		//				client.reqInfo(cf3);
		//
		//			default:
		//				printHelp();
		//				break;
		//			}
		//
		//		}
		//
		//
		//		in.close();
		//
		//		System.exit(0);


		//		client.getCommand();


		// TODO: by shutdown command. 
		/* Don't quit until we've disconnected: */
		//		logger.trace("Shutting down");
		//		workerGroup.shutdownGracefully();

	}

	//
	//		Scanner in = new Scanner(System.in);
	//		System.out.println("Enter a command: ");
	//
	//		String inputLine = "";
	//		inputLine = in.nextLine();
	//
	//		if (inputLine.equals("exit")) {
	//			System.out.println("Bye.");
	//			System.exit(0);
	//		}
	//		String[] line = inputLine.split(" ");
	//		if (line.length != 2 && !inputLine.equals("info")) {
	//			printHelp();
	//		}
	//
	//		String command = line[0];
	//		switch (command) {
	//		case "store":
	//			logger.trace("store command executed!");
	//			inputFileName = line[1];
	//
	//			// Set up Netty. 
	//			ChannelFuture cf = setupNettyClient(
	//					client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
	//			client.requestListOfSNsToStore(cf);
	//
	//			//				System.out.println(inputFileName + " saved!");
	//			logger.trace("store command end.");
	//			break;
	//
	//		case "retrieve":
	//			logger.trace("retrieve command executed!");
	//			outputFileName = line[1];
	//
	//			// Set up Netty. 
	//			ChannelFuture cf2 = setupNettyClient(
	//					client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
	//			client.requestListOfSNsToRetrieve(cf2);
	//
	//			//				System.out.println(outputFileName + " retrieved!");
	//			logger.trace("retrieve command end.");
	//			break;
	//
	//		case "info":
	//
	//			// Set up Netty. 
	//			ChannelFuture cf3 = setupNettyClient(
	//					client, "controller", CONTROLLER_IP_ADDRESS, CONTROLLER_PORT);
	//			client.reqInfo(cf3);
	//
	//		default:
	//			printHelp();
	//			break;
	//		}
	//
	//
	//		in.close();
	//
	//	}











}
































