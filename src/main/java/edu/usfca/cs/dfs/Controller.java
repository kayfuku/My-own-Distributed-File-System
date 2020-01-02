package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelHandlerContext;

public class Controller {
	private static Logger logger = LogManager.getLogger();

	private static ServerMessageRouter messageRouter;
	private static int port;
	private static Map<String, StorageInfo> addrToStorageInfo = new HashMap<>();
	private static List<String> listSNsAlive = new LinkedList<>();
	private static Random rand = new Random();
	private static Map<String, Integer> fileNameToNumChunks = new HashMap<>();
	private static Map<String, Integer> snToNumRequests = new HashMap<>();
	private static Map<String, Float> snToSpace = new HashMap<>();


	public Controller() {

	}


	public void start(Controller controller) throws IOException {
		messageRouter = new ServerMessageRouter(controller, null);
		messageRouter.listen(port);
		logger.trace("Listening for connections on port " + port);
		System.out.println("Listening for connections on port " + port);

		// Failure detector. 
		FailureDetector failureDetector = new FailureDetector(listSNsAlive, addrToStorageInfo);
		Thread thread = new Thread(failureDetector);
		thread.start();
	}


	public void getStorageAddress(ChannelHandlerContext ctx, StorageMessageWrapper msg) {
		// Controller receives addresses from SN. 
		StorageMessages.StorageAddr storageAddrMsg = msg.getStorageAddrMsg();
		String addrSn = storageAddrMsg.getStorageAddr();
		float space = storageAddrMsg.getSpace();
		int numRequests = storageAddrMsg.getNumReq();
		logger.trace("[Controller] Got address! " + addrSn);

		InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
		String addrSnClient = addr.toString().substring(1);
		long currentTime = System.nanoTime();

		addrToStorageInfo.put(addrSn, new StorageInfo(addrSn, addrSnClient, currentTime)); 
		snToSpace.put(addrSn, space);
		snToNumRequests.put(addrSn, numRequests);
		listSNsAlive.add(addrSn);
		logger.trace("[Controller] addrToStorageInfo: " + addrToStorageInfo);
	}


	public void getHeartbeat(ChannelHandlerContext ctx, StorageMessageWrapper msg) {
		// Controller receives heartbeats from SN. 
		StorageMessages.Heartbeat heartbeatMsg = msg.getHeartbeatMsg();
		String heartbeatStr = heartbeatMsg.getHeartbeat();
		float space = heartbeatMsg.getSpace();
		int numRequests = heartbeatMsg.getNumReq();
		logger.trace("[Controller] Receive " + heartbeatStr);

		// Update info such as last heartbeat time, space available, and num of requests. 
		InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
		String addrSnClient = addr.toString().substring(1);
		logger.trace("addrSnClient: " + addrSnClient);
		logger.trace("listSNsAlive: " + listSNsAlive);
		long currentTime = System.nanoTime();
		for (String addrAlive : listSNsAlive) {
			StorageInfo snInfo = addrToStorageInfo.get(addrAlive);
			String address = snInfo.getAddress();
			if (addrSnClient.equals(snInfo.getAddrSnClient())) {
				logger.trace("Update last heartbeat time in SN, " + addrAlive);
				snInfo.setLastHeartbeatTime(currentTime);
				snToSpace.put(address, space);
				snToNumRequests.put(address, numRequests);
			}
		}
	}


	public void getStorageReq(ChannelHandlerContext ctx, StorageMessageWrapper msg) {
		// Controller receives a store request from Client. 
		StorageMessages.StoreReq storeReqMsg = msg.getStoreReqMsg();
		String fileName = storeReqMsg.getFileName();
		int numChunks = storeReqMsg.getNumChunks();
		fileNameToNumChunks.put(fileName, numChunks);
		logger.trace("numChunks: " + numChunks);

		// Pick up SNs to store chunks. 
		List<String> addressesPicked = getAddressesToStore(numChunks, fileName);
		logger.trace("addressesPicked: " + addressesPicked);

		// Create a response. 
		StorageMessages.StoreRes storeResMsg = StorageMessages.StoreRes.newBuilder()
				.addAllAddresses(addressesPicked)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setStoreResMsg(storeResMsg)
				.build();			

		// Send it back to Client. 
		logger.trace("[Controller] Sending back the list of SNs.");
		ctx.channel().writeAndFlush(msgWrapper);
		ctx.close();
	}


	// Pick up SNs to store chunks. 
	private List<String> getAddressesToStore(int numChunks, String fileName) {
		logger.trace("listSNsAlive: " + listSNsAlive);

		List<String> addressesPicked = new LinkedList<>();
		int len = listSNsAlive.size();
		if (numChunks >= len) {
			addressesPicked = listSNsAlive;
		} else {
			rand = new Random();
			int index = rand.nextInt(len);
			int i = 0;
			while (i < numChunks) {
				addressesPicked.add(listSNsAlive.get(index % len));
				index++;
				i++;
			}
		}

		putInBloomFilter(fileName, addressesPicked);


		return addressesPicked;
	}


	private void putInBloomFilter(String fileName, List<String> addressesPicked) {
		for (String addr : addressesPicked) {
			StorageInfo snInfo = addrToStorageInfo.get(addr);
			snInfo.putInBF(fileName);
		}
	}


	public void getRetrieveReq(ChannelHandlerContext ctx, StorageMessageWrapper msg) {
		// Controller receives a retrieve request from Client. 
		StorageMessages.RetrieveReq retrieveReqMsg = msg.getRetrieveReqMsg();
		String fileName = retrieveReqMsg.getFileName();
		int numChunks = fileNameToNumChunks.get(fileName);
		logger.trace("fileName: " + fileName);
		logger.trace("numChunks: " + numChunks);

		// Check Bloom Filter in each SN to retrieve chunks from.  
		List<String> addressesPicked = getAddressesToRetrieve(fileName);
		logger.trace("addressesPicked: " + addressesPicked);

		// Create a response. 
		StorageMessages.RetrieveRes retrieveResMsg = StorageMessages.RetrieveRes.newBuilder()
				.addAllAddresses(addressesPicked)
				.setNumChunks(numChunks)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setRetrieveResMsg(retrieveResMsg)
				.build();			

		// Send it back. 
		logger.trace("[Controller] Sending back the list of SNs.");
		ctx.channel().writeAndFlush(msgWrapper);
		ctx.close();
	}


	private List<String> getAddressesToRetrieve(String fileName) {
		logger.trace("listSNsAlive: " + listSNsAlive);

		// Check Bloom Filter in each SN. 
		List<String> addressesPicked = new LinkedList<>();
		for (String addr : listSNsAlive) {
			StorageInfo snInfo = addrToStorageInfo.get(addr);
			if (snInfo.getFromBF(fileName)) {
				addressesPicked.add(addr);
			}
		}

		return addressesPicked;		
	}


	public void getInfo(ChannelHandlerContext ctx, StorageMessageWrapper msg) {
		// Controller receives a store request from Client. 
		StorageMessages.InfoReq infoReqMsg = msg.getInfoReqMsg();
		String message = infoReqMsg.getInfoReq();
		logger.trace("numChunks: " + message);

		// Calculate space available. 
		float space = 0;
		for (float s : snToSpace.values()) {
			space += s;
		}

		// Create a response. 
		StorageMessages.InfoRes infoResMsg = StorageMessages.InfoRes.newBuilder()
				.addAllAddresses(listSNsAlive)
				.setSpace(space)
				.putAllSnToNumRequests(snToNumRequests)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setInfoResMsg(infoResMsg)
				.build();			

		// Send it back to Client. 
		logger.trace("[Controller] Sending back the list of SNs.");
		ctx.channel().writeAndFlush(msgWrapper);
		ctx.close();
	}


	public static void main(String[] args) throws IOException {
		port = Integer.parseInt(args[0]);
		Controller s = new Controller();
		s.start(s);
	}


















}















































































