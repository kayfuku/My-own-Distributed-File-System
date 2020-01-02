package edu.usfca.cs.dfs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

public class Lab {

	private static String INPUT_FILE_PATH = "data/input/";
//	private static String INPUT_FILE_NAME = "my_file.txt";
//	private static String INPUT_FILE_NAME = "file_469KB.jpg";
	private static String INPUT_FILE_NAME = "image.jpg";
	private static int CHUNK_SIZE = 5;


	public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InterruptedException {

		// Test num of chunks. 
		File inputFile = new File(INPUT_FILE_PATH + INPUT_FILE_NAME);
		System.out.println("file size: " + inputFile.length());
		int numChunks = (int) Math.ceil((double) inputFile.length() / CHUNK_SIZE);
		System.out.println("numChunks: " + numChunks);


		// Test LZ4. 
//		byte[] binary = Files.readAllBytes(Paths.get("data/file_469KB.jpg"));
//		System.out.println("file size: " + binary.length);
//		// Compress. 
//		LZ4Factory factory = LZ4Factory.fastestInstance();
//		LZ4Compressor compressor = factory.fastCompressor();
//		byte[] compressed = compressor.compress(binary);
//		System.out.println("compressed size: " + compressed.length);
//		// Decompress. 
//		LZ4FastDecompressor decompressor = factory.fastDecompressor();
//		byte[] decompressed = decompressor.decompress(compressed, binary.length);
//		System.out.println("decompressed size: " + decompressed.length);


		// Test calculating entropy. 
		byte[] test = Files.readAllBytes(Paths.get(INPUT_FILE_PATH + INPUT_FILE_NAME));
		double entr = entropy(test);
		System.out.println("Optimal Size Reduction: "
				+ String.format("%.0f%%", (1 - (entr / 8)) * 100));


		// Test checksum. 
		MessageDigest md = MessageDigest.getInstance("MD5");
		//      String hex = checksum("data/my_file.txt", md);
		byte[] bytes = new byte[]{ 23, 45, 35, 22, 19 };
		String hex = checksum(bytes, md);
		System.out.println(hex);


		// Test time lapse. 
		long start = System.nanoTime();
		calculation();
		long end = System.nanoTime();
		// time elapsed
		long output = end - start;
		System.out.println("Time elapsed in milliseconds: " + output / 1000000);


		// Test numChunks. 
		File inputFile2 = new File(INPUT_FILE_PATH + "image.jpg");
		int totalNumChunks = (int) Math.ceil((double) inputFile2.length() / CHUNK_SIZE);
		System.out.println("totalNumChunks: " + totalNumChunks);






	}



	/**
	 * Calculates the entropy per character/byte of a byte array.
	 *
	 * @param input array to calculate entropy of
	 *
	 * @return entropy bits per byte
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



	private static String checksum(byte[] bytes, MessageDigest md) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
			byte[] buffer = new byte[1024];
			int k = 0;
			while ((k = bais.read(buffer)) != -1) {
				md.update(buffer, 0, k);
			}
		}

		// Convert bytes to hex. 
		StringBuilder sb = new StringBuilder();
		for (byte b : md.digest()) {
			sb.append(String.format("%02x", b));
		}

		return sb.toString();
	}



	private static void calculation() throws InterruptedException {
		// Sleep 2 seconds. 
		TimeUnit.SECONDS.sleep(2);
	}




}
































