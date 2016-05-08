package edu.upenn.cis455.mapreduce.worker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import edu.upenn.cis455.mapreduce.Context;

public class PageRankReduceContext implements Context {
	
	private File outputDir;
	private int workerID;

	public PageRankReduceContext(File outputDir, int workerID) {
		this.outputDir = outputDir;
		this.workerID = workerID;
	}

	@Override
	public void write(String key, String value) {

		try {
			File outputfile = new File(this.outputDir, "worker-"+this.workerID);
			if (!outputfile.exists() || !outputfile.isFile()) {
				outputfile.createNewFile();
			}
			
			PrintWriter rankFileWriter = new PrintWriter(new FileWriter(outputfile, true));
			rankFileWriter.println(key + "\t" + value);
			rankFileWriter.flush();
			rankFileWriter.close();
			
		} catch (IOException e) {
		}
		
		
	}

}
