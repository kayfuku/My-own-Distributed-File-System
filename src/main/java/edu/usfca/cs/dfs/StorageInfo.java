package edu.usfca.cs.dfs;

public class StorageInfo {
	private String address;
	private int port;
	private String addrSnClient;
	private BloomFilter bloomFilter;
	private long lastHeartbeatTime;
	private boolean isAlive;
	
	public StorageInfo(String address, String addrSnClient, long time) {
		this.address = address;
		this.port = Integer.parseInt(address.split(":")[1]);
		this.addrSnClient = addrSnClient;
    	this.bloomFilter = new BloomFilter(20, 3);
    	this.lastHeartbeatTime = time;
    	this.isAlive = true;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public int getPort() {
		return port;
	}
	public String getAddrSnClient() {
		return addrSnClient;
	}
	public void setAddrSnClient(String addrSnClient) {
		this.addrSnClient = addrSnClient;
	}
	public BloomFilter getBloomFilter() {
		return bloomFilter;
	}
	public void setIsAlive(boolean b) {
		this.isAlive = b;
	}
	public long getLastHeartbeatTime() {
		return lastHeartbeatTime;
	}
	public void setLastHeartbeatTime(long lastHeartbeatTime) {
		this.lastHeartbeatTime = lastHeartbeatTime;
	}
	public boolean isAlive() {
		return isAlive;
	}
	
	public void putInBF(String fileName) {
		bloomFilter.put(fileName.getBytes());
	}
	
	public boolean getFromBF(String fileName) {
		return bloomFilter.get(fileName.getBytes());
	}
	
	
	
	
	
}






















