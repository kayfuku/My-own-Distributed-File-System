package edu.usfca.cs.dfs;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FailureDetector implements Runnable {
	private static Logger logger = LogManager.getLogger();

	private List<String> listSNsAlive;
	private Map<String, StorageInfo> addrToStorageInfo;
	private int HEARTBEAT_INTERVAL = 5;
	private int DELAY = 3;
	private boolean running = true;
	
	public FailureDetector(List<String> listSNsAlive, Map<String, StorageInfo> addrToStorageInfo) {
		this.listSNsAlive = listSNsAlive;
		this.addrToStorageInfo = addrToStorageInfo;
	}
	
	@Override
	public void run() {
		logger.trace("FailureDetector run() start.");
		
		while (running) {

			if (!listSNsAlive.isEmpty()) {
				logger.trace("FailureDetector checking... ");

				for (String addr : listSNsAlive) {
					long lastHeartbeatTime = addrToStorageInfo.get(addr).getLastHeartbeatTime();
					long currentTime = System.nanoTime();
					long elapse = currentTime - lastHeartbeatTime;
					logger.trace(addr + " elapse: " + elapse / 1000000);
					if (elapse / 1000000 > (HEARTBEAT_INTERVAL + DELAY) * 1000) {
						
						detectFailure(addr);
						
					}
				}
			}
			
			try {
				Thread.sleep(HEARTBEAT_INTERVAL * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	
	public void detectFailure(String addr) {
		logger.trace("detectFailure() start.");
		
		logger.trace("addr: " + addr);
		logger.trace("addrToStorageInfo: " + addrToStorageInfo);

		if (addrToStorageInfo.containsKey(addr)) {
			logger.trace("Detect Storage Node, " + addr + " down!");
			
			// Maintain SNs statuses. 
			listSNsAlive.remove(addr);
			logger.trace("listSNsAlive after removing: " + listSNsAlive);
			StorageInfo snInfo = addrToStorageInfo.get(addr);
			snInfo.setIsAlive(false);
		}
	}
	
	
	
	

}





























