package edu.usfca.cs.dfs;

import java.util.Random;

public class TestBloomFilter {

	public static void main(String[] argv) {
		BloomFilter filter = new BloomFilter(20, 3);    
		
		
		////// kei start //////
		String str1 = "apple";
		String str2 = "banana";
		String str3 = "orange";
		String str4 = "kiwi";
		
		byte[] data1 = str1.getBytes();
		byte[] data2 = str2.getBytes();
		byte[] data3 = str3.getBytes();
		byte[] data4 = str4.getBytes();

		filter.put(data1);
		filter.put(data2);
		filter.put(data3);

		System.out.println("Check membership: " + filter.get(data1));
		System.out.println("Check membership: " + filter.get(data2));
		System.out.println("Check membership: " + filter.get(data3));
		System.out.println("Check membership: " + filter.get(data4));
		////// kei end //////

		
		
		filter = new BloomFilter(20, 3);    
		byte[] data = new byte[20];
		int n = 3;
		for (int i = 0; i < n; i++) {
			new Random().nextBytes(data);
			filter.put(data);
		}
		System.out.println("After put " + n + " elements");

		System.out.println("Get a exist data: " + filter.get(data));
		new Random().nextBytes(data);
		System.out.println("Get a not exist data: " + filter.get(data));
		System.out.println("Current False Positive Prob: " + filter.falsePositiveProb());






	}
}