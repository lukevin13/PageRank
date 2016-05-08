package edu.upenn.cis455.mapreduce.worker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import edu.upenn.cis455.mapreduce.Context;

public class PageRankMapContext implements Context {

	private File spoolIn;	// Directory for reduce to read from
	private int numWorkers;
	private List<String> workerList;
	private int self;

	public PageRankMapContext(int self, File spoolIn, int numWorkers, List<String> workerList) {
		this.numWorkers = numWorkers;
		this.spoolIn = spoolIn;
		this.workerList = workerList;
		this.self = self;
	}

	@Override
	public void write(String key, String value) {
		int workerID = assignToWorker(key, this.numWorkers);
		File dir = new File(this.spoolIn, this.workerList.get(workerID));
		if (!dir.exists() || !dir.isDirectory()) {
			dir.mkdirs();
		}
		File file = new File(dir, "worker-" + this.self);

		try {
			if (!file.exists() || !file.isFile()) {
				file.createNewFile();
			}

			PrintWriter writer = new PrintWriter(new FileWriter(file, true));
			writer.println(key + "\t" + value);
			writer.flush();
			writer.close();
		} catch (IOException e) {
		}
	}

	/*Helper function that hashes the given key, and assigns it to a worker.
	The function tries to distribute hashed keys as evenly as possible using
	the equation hash.mod(numberOfWorkers)*/
	public int assignToWorker(String key, int numWorkers) { 
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-1");
			digest.update(key.getBytes());
			byte[] result = digest.digest();
			BigInteger hashNum = new BigInteger(1, result);

			BigInteger numBuckets = new BigInteger(Integer.toString(numWorkers));
			int i = hashNum.mod(numBuckets).intValue();
			return i;
		} catch (NoSuchAlgorithmException e) {
			return 0;
		}		
	}
}
