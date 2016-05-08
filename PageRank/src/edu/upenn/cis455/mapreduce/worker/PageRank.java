package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis455.mapreduce.job.PageRankJob;

public class PageRank {

	// Folders
	private File inputDir;
	private File outputDir;
	private File spoolIn;
	private File spoolOut;
	private File corpusZipped;
	private File corpusUnzipped;

	// Files
	private File inputFile;
	private File outputFile;

	// Tracking
	private int workerID;
	private int numWorkers;
	private List<String> workerList;
	private PageRankJob prj;
	private PageRankMapContext prmc;
	private PageRankReduceContext prrc;

	// S3 folder keys
	private String s3CorpusDirKey = "test_docs/";
	private String s3SpoolInDirKey = "pagerank/storage/spoolIn/";
	private String s3SpoolOutDirKey = "pagerank/storage/spoolOut/";
	private String s3OutputDirKey = "pagerank/storage/output/";

	// Constructor
	public PageRank(String root, int numWorkers, List<String> workerList) {

		// Declare the directories
		this.inputDir = new File(root, "Input");
		this.outputDir = new File(root, "Output");
		this.spoolIn = new File(root, "SpoolIn");
		this.spoolOut = new File(root, "SpoolOut");
		this.corpusZipped = new File(root, "CorpusZipped");
		this.corpusUnzipped = new File(root, "CorpusUnzipped");

		// Declare the files
		this.inputFile = new File(this.inputDir, "Input");
		this.outputFile = new File(this.outputDir, "Output");

		// Make the directories
		FileHelper.makeDirDeleteIfExists(this.spoolIn);
		FileHelper.makeDirDeleteIfExists(this.spoolOut);

		// Workers
		this.numWorkers = numWorkers;
		this.workerList = workerList;

		// Jobs and Contexts
		this.prj = new PageRankJob();
		this.prmc = new PageRankMapContext(this.workerID, this.spoolIn, this.numWorkers, this.workerList);
		this.prrc = new PageRankReduceContext(this.outputDir, this.workerID);
	}

	// Retrieves the corpus from S3, unzips them, then convert documents to input file
	public void runCorpusTasks(String s3Files) {
		if (s3Files == null || s3Files.isEmpty()) {
			return;
		}
		// Create the directories, delete if they exists already
		FileHelper.makeDirDeleteIfExists(this.corpusZipped);
		FileHelper.makeDirDeleteIfExists(this.corpusUnzipped);

		// Create list of files to download
		String[] parsed = s3Files.split(",");
		List<String> filelist = new ArrayList<String>();
		for (String s : parsed) {
			if (s != null && !s.isEmpty()) {
				filelist.add(s);
			}
		}

		// Download files from the S3 corpus directory
		S3Wrapper s3 = new S3Wrapper();
		List<String> zippedFiles = s3.getFileList(this.s3CorpusDirKey);
		for (String s3filekey : zippedFiles) {
			String s3filename = s3filekey.replace(this.s3CorpusDirKey, "");
			if (s3filename != null && !s3filename.isEmpty()) {
				//				if (filelist.contains(s3filename)) {
				File localfile = new File(this.corpusZipped, s3filename);
				FileHelper.touchDeleteIfExists(localfile);
				s3.download(s3filekey, localfile);
				//				}
			}
		}

		// Unzip and delete files in the local zipped corpus
		for (File zippedFile : this.corpusZipped.listFiles()) {
			Zip4jWrapper unzipper = new Zip4jWrapper();
			unzipper.unzip(zippedFile, this.corpusUnzipped);
			zippedFile.delete();
		}

		// Create the input directory
		FileHelper.makeFile(this.inputFile);

		// Convert raw html documents to map input format
		for (File htmlDir : this.corpusUnzipped.listFiles()) {
			for (File htmlDoc : htmlDir.listFiles()) {
				if (htmlDoc != null && htmlDoc.isFile()) {
					try {
						// Read file and extract url and html content
						BufferedReader reader = new BufferedReader(new FileReader(htmlDoc));
						String url = reader.readLine();	// Get the page url
						if (url == null) {
							reader.close();
							continue;
						}
						url = URLEncoder.encode(url, "UTF-8");
						reader.readLine();				// Skip this line

						// Get the page content
						String pageContent = "";
						String line = "";
						while ((line = reader.readLine()) != null) {
							pageContent += line;
						}

						// Use JSoup to get the href links in the document
						Document doc = Jsoup.parse(pageContent);
						if (doc == null) {
							reader.close();
							continue;
						}
						Elements elements = doc.select("a[href]");
						if (elements == null) {
							reader.close();
							continue;
						}
						String links = "";
						for (Element element : elements) {
							String link = element.attr("abs:href");
							if (link != null && !link.isEmpty()) {
								link = URLEncoder.encode(link, "UTF-8");
								links += link + ",";
							}
						}
						if (!links.isEmpty()) {
							links = links.substring(0, links.length()-1);
						}
						reader.close();

						// Writer the url, initial pagerank, and out links to the input file
						PrintWriter writer = new PrintWriter(new FileWriter(this.inputFile, true));
						writer.println(url + ":1.0" + "\t" + links);
						writer.close();

					} catch (FileNotFoundException e) {
					} catch (IOException e) {
					}
				}
			}
		}
	}

	// Map
	public void runMap() {
		if (this.inputFile == null || !this.inputFile.exists() || !this.inputFile.isFile()) {
			return;
		}
		try {
			// Read and run map on each nonempty line in the input file
			BufferedReader reader = new BufferedReader(new FileReader(this.inputFile));
			String line = "";
			while ((line = reader.readLine()) != null) {
				if (!line.isEmpty()) {
					String[] parsedLine = line.split("\t", 2);
					if (parsedLine[1].isEmpty()) {
						continue;
					}
					this.prj.map(parsedLine[0], parsedLine[1], this.prmc);
				}
			}
			reader.close();

		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}
	}

	// Shuffle and sort
	public void runShuffleSort() {
		S3Wrapper s3 = new S3Wrapper();

		// Upload the spool in directory and its files to S3
		for (File localFileDir : this.spoolIn.listFiles()) {
			if (localFileDir != null && localFileDir.isDirectory()) {
				for (File localFile : localFileDir.listFiles()) {

					if (localFile != null && localFile.isFile()) {
						String s3Filekey = this.s3SpoolInDirKey + localFileDir.getName() + localFile.getName();
						s3.upload(s3Filekey, localFile);
					}
				}
			}
		}
	}

	// Download the spool in files from S3
	String s3FileDirKey = this.s3SpoolOutDirKey + this.workerList.get(this.workerID) + "/";
	for (String s3FileKey : s3.getFileList(s3FileDirKey)) {
		String s3Filename = this.s3FileKey.replace(this.s3FileDirKey, "");
		if (!s3Filename.isEmpty()) {
			File localFileDir = new File(this.spoolOut, this.workerList.get(this.workerID));
			File localFile = new File(localFileDir, this.s3FileKey);
			FileHelper.makeFile(localFile);
			s3.download(this.s3FileKey, localFile);
		}
	}
}

// Reduce
public void runReduce() {
	FileHelper.makeFile(this.outputFile);
	File spoolOutFileDir = new File(this.spoolOut, this.workerList.get(this.workerID));

	// Collect the values on each line and produce a values list for the reduce job
	if (spoolOutFileDir != null && spoolOutFileDir.isDirectory()) {
		Map<String, List<String>> reduceMap = new HashMap<String, List<String>>();
		for (File spoolFile : spoolOutFileDir.listFiles()) {
			if (spoolFile.isFile()) {
				try {
					BufferedReader reader = new BufferedReader(new FileReader(spoolFile));
					String line = "";
					while ((line = reader.readLine()) != null) {
						if (!line.isEmpty() && line.contains("\t")) {
							String[] parsed = line.split("\t", 2);
							String key = parsed[0];
							String value = parsed[1];
							if (!reduceMap.containsKey(key)) {
								reduceMap.put(key, new ArrayList<String>());
							}
							List<String> values = reduceMap.get(key);
							values.add(value);
							reduceMap.put(key, values);
						}
					}
					reader.close();

				} catch (FileNotFoundException e) {
				} catch (IOException e) {
				}
			}
		}

		// Run reduce on each entry in the map
		for (String key : reduceMap.keySet()) {
			this.prj.reduce(key, reduceMap.get(key).toArray(new String[reduceMap.size()]), this.prrc);
		}

		// Upload each of the output files of the workers onto s3
		S3Wrapper s3 = new S3Wrapper();
		for (File file : this.outputDir.listFiles()) {
			if (file.isFile()) {
				String uploadKey = this.s3OutputDirKey + "worker-" + this.workerID;
				s3.upload(uploadKey, file);
			}
		}

		// Copy output file to input directory for next iteration
		FileHelper.makeDirDeleteIfExists(this.inputDir);
		FileHelper.touchDeleteIfExists(this.inputFile);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(this.outputFile));
			PrintWriter writer = new PrintWriter(new FileWriter(this.inputFile));
			String line = "";
			while ((line = reader.readLine()) != null) {
				if (!line.isEmpty()) {
					writer.println(line);
				}
			}
			reader.close();
			writer.close();
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}
	}
}
}
