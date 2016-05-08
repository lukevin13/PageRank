package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.tidy.Tidy;

import edu.upenn.cis455.mapreduce.job.PageRankJob;

public class OldPageRank {

	private File inputDir;
	private File outputDir;
	private File spoolin;
	private File spoolout;
	private File batch;
	private File zips;

	private Map<Integer, String> workerMap;
	private PageRankJob prj;
	private PageRankMapContext prmc;
	private PageRankReduceContext prrc;

	//	private String bdbDir = "";

	private int workerID;

	// Runs an iteration of Map Reduce
	public OldPageRank(String root, int numWorkers, int workerID, Map<Integer, String> workerMap) {
		this.workerID = workerID;

		// Declare Files
		if (root != null) {
			File rootDir = new File(root);
			if (!rootDir.exists() || !rootDir.isDirectory()) {
				rootDir.mkdirs();
			}
		}
		this.inputDir = new File(root, "input");
		this.outputDir = new File(root, "output");
		this.spoolin = new File(root, "PageRankSpoolin");
		this.spoolout = new File(root, "PageRankSpoolout");
		this.batch = new File(root, "batch");
		this.zips = new File(root, "zips");
		this.workerMap = workerMap;
		//		this.bdbDir = this.bdbDir;

		// Create directories and files
		this.spoolin = FileHelper.makeDirDeleteIfExists(this.spoolin);
		this.spoolout = FileHelper.makeDirDeleteIfExists(this.spoolout);
//		this.inputDir = FileHelper.makeDirDeleteIfExists(this.inputDir);
		this.outputDir = FileHelper.makeDirDeleteIfExists(this.outputDir);
		if (!this.zips.exists() || !this.zips.isDirectory()) {
			this.zips.mkdirs();
		}
		this.prj = new PageRankJob();
//		this.prmc = new PageRankMapContext(this.workerID, this.spoolin, numWorkers, this.workerMap);
	}

	// Shuffles
	public void shufflePhase() {
		for (File dir : this.spoolin.listFiles()) {
			if (dir.isDirectory()) {
				for (File f : dir.listFiles()) {
					S3Wrapper s3 = new S3Wrapper();
					String key = "pagerank/storage/" + dir.getName() + "/" + f.getName();
					s3.upload(key , f);
				}
			}
		}
	}

	// Sorts 
	public void sortPhase() {
		S3Wrapper s3 = new S3Wrapper();
		String key = "pagerank/storage/" + this.workerMap.get(this.workerID) + "/";
		for (String fstr : s3.getFileList(key)) {
			String nfile = fstr.replace(key, "");
			if (!nfile.isEmpty() && !nfile.contains("/")) {
				File file = new File(this.spoolout, nfile);
				file = FileHelper.touchDeleteIfExists(file);
				s3.download(key + nfile, file);
			}
		}
	}

	// gets the document files
	public void retrievePhase(String files) {
		if (files != null && !files.isEmpty()) {
			for (String filename : files.split(",")) {
				String key = "doc_content/" + filename;
				File file = new File(this.zips, filename);
				File pfile = new File(file.getParentFile().getPath());
				pfile.mkdirs();
				file = FileHelper.touchDeleteIfExists(file);
				S3Wrapper s3 = new S3Wrapper();
				s3.download(key, file);
			}
		}
		unzipPhase();
	}

	// unzips document files
	public void unzipPhase() {
		for (File f : this.zips.listFiles()) {
			if (f.isFile()) {
				Zip4jWrapper zWrapper = new Zip4jWrapper();
				zWrapper.unzip(f, this.batch);
			}
		}
	}

	// Converts documents to a rank file
	public void convertPhase() {
		this.inputDir = FileHelper.makeDirDeleteIfExists(this.inputDir);
		try {
			File input = new File(this.inputDir, "worker-" + this.workerID);
			input = FileHelper.touchDeleteIfExists(input);
			PrintWriter writer = new PrintWriter(new FileWriter(input), true);
			if (this.batch.exists() && this.batch.isDirectory()) {
				for (File dir : this.batch.listFiles()) {
					if (dir.isDirectory()) {
						for (File f : dir.listFiles()) {
							if (f.isFile()) {
								BufferedReader reader = new BufferedReader(new FileReader(f));

								String url = reader.readLine();	// Get url
								reader.readLine();					// Skip type
								if (url != null && !url.isEmpty()) {
									writer.print(URLEncoder.encode(url.trim(),"UTF-8") + "::1.0\t");
								} else {
									continue;
								}
								// Get html content
								String content = "";
								String line = "";
								while ((line = reader.readLine()) != null) {
									content += line;
								}

								reader.close();
								writer.println(findLinks(content, new URL(url)));
								writer.flush();
							}
						}
					}
				}
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
		} 
	}

	// Map
	public void mapPhase() {
		this.spoolin = FileHelper.makeDirDeleteIfExists(this.spoolin);
		this.spoolout = FileHelper.makeDirDeleteIfExists(this.spoolout);
		try {
			for (File f : this.inputDir.listFiles()) {
				if (f.isFile()) {
					BufferedReader reader = new BufferedReader(new FileReader(f));
					String line = "";
					while ((line = reader.readLine()) != null) {
						if (line.isEmpty()) {
							continue;
						}
						String[] parsed = line.split("\t", 2);
						String key = parsed[0];
						String value = parsed[1];

						if (!value.isEmpty()) {
							this.prj.map(key, value, this.prmc);
						}
					}
					reader.close();
				}
			}
		} catch (IOException e) {
		}
	}

	// Reduce
	public void reducePhase() {
		// Create reduce context
		this.outputDir = FileHelper.makeDirDeleteIfExists(this.outputDir);
		this.prrc = new PageRankReduceContext(this.outputDir, this.workerID);

		try {
			Map<String, List<String>> terms = new HashMap<String, List<String>>();
			Map<String, String> outlinks = new HashMap<String, String>();
			for (File f : this.spoolout.listFiles()) {
				if (f.isFile()  && f.getName().equals("worker-"+this.workerID)) {
					BufferedReader freader = new BufferedReader(new FileReader(f));
					String foutLinks = "";
					String fline = "";
					while ((fline = freader.readLine()) != null) {
						if (fline.isEmpty()) {
							continue;
						}
						String[] fparsed = fline.split("\t", 2);
						String fkey  = fparsed[0];
						String fvalue = fparsed[1];
						
						if (fkey.isEmpty()) {
							continue;
						}

						// Add to maps if key is new
						if (!terms.keySet().contains(fkey)) {
							terms.put(fkey, new ArrayList<String>());
						}
						if (!outlinks.keySet().contains(fkey)) {
							outlinks.put(fkey, "");
						}

						// Add to maps
						if (fvalue.startsWith("->")) {
							foutLinks = fvalue.substring(2);
							outlinks.put(fkey, foutLinks);
						} else {
							terms.get(fkey).add(fvalue);
						}
					}




					freader.close();

				}
			}

			// Combine maps
			for (String k : terms.keySet()) {
				terms.get(k).add(outlinks.get(k));
			}

			// reduce
			for (String k :terms.keySet()) {
				List<String> values = terms.get(k);
				this.prj.reduce(k, values.toArray(new String[values.size()]), this.prrc);
			}
		} catch (IOException e) {
		}
	}

	// Sends rankFile to master
	public void collectPhase() {
		for (File f: this.outputDir.listFiles()) {
			if (f.isFile()) {
				S3Wrapper s3 = new S3Wrapper();
				String key = "pagerank/storage/output/" + f.getName();
				s3.upload(key, f);
			}
		}
	}

	// Updates local RankFile
	public void share() {
		this.inputDir = FileHelper.makeDirDeleteIfExists(this.inputDir);
		S3Wrapper s3 = new S3Wrapper();
		String key = "pagerank/storage/output/";
		for (String fstr : s3.getFileList(key)) {
			String nfile = fstr.replace(key, "");
			if (!nfile.isEmpty() && !nfile.contains("/")) {
				File file = new File(this.inputDir, nfile);
				file = FileHelper.touchDeleteIfExists(file);
				s3.download(key + nfile, file);
			}
		}
	}

	// Converts string HTML data to DOM with JTidy and then extracts links
	public String findLinks(String html, URL url) throws UnsupportedEncodingException {
		List<String> links = new ArrayList<String>();

		// Create JTidy Object
		Tidy tidy = new Tidy();
		tidy.setXHTML(true);
		tidy.setShowErrors(0);
		tidy.setShowWarnings(false);
		tidy.setQuiet(true);


		// Parse the documents
		Document doc = null;
		try {
			doc = tidy.parseDOM(new ByteArrayInputStream(html.getBytes()), null);
		}catch (StringIndexOutOfBoundsException e) {
			return "";
		}

		if (doc == null) {
			return "";
		}

		NodeList elements = doc.getElementsByTagName("a");
		if (elements == null) {
			return "";
		}
		// Extract the href links
		for (int i = 0; i < elements.getLength(); i++) {
			Node node = elements.item(i);
			if (node == null) {
				continue;
			}
			NamedNodeMap attributes = node.getAttributes();
			if (attributes == null) {
				continue;
			}

			boolean flag = false;
			try {
				flag = (attributes.getNamedItem("href") != null);
			} catch (NullPointerException e) {
			}
			if (flag) {

				String link = attributes.getNamedItem("href").getNodeValue();

				if (link == null || link.isEmpty()) {
					continue;
				}

				if (isAbsolutePath(link)) {
					links.add(link);
				} else {
					String path = url.getPath();
					if ((link.charAt(0) != '/' && path.isEmpty()) ||
							(link.charAt(0) != '/' && !path.endsWith("/"))) {
						link = "/" + link;
					}

					String oflink = url.getProtocol() + "://" + url.getHost() + link;

					links.add(URLEncoder.encode(oflink, "UTF-8"));
				}
			}
		}

		//		BDBWrapper bdb = new BDBWrapper(this.bdbDir);
		//		String title = getDocTitle(doc);

		String outlinks = "";
		for (String s : links) {
			if (s != null && !"".equals(s.trim())) {
				outlinks += (s + ",");
			}
		}
		if (outlinks.length() != 0) {
			return outlinks.substring(0, outlinks.length()-1);
		} else {
			return "";
		}
	}

	private boolean isAbsolutePath(String link) {
		return link.matches("^http://.*") || link.matches("^https://.*");
	}

	private String getDocTitle(Document doc) {
		if (doc == null) {
			return "";
		}
		String title = "";
		NodeList elements = doc.getElementsByTagName("title");
		if (elements == null) {
			return "";
		}
		for (int i = 0; i < elements.getLength(); i++) {
			NodeList nl = elements.item(i).getChildNodes();
			if (nl == null) {
				return "";
			}
			for (int j = 0; j < nl.getLength(); j++) {
				title = nl.item(j).getNodeValue();
			}
		}
		if (title == null) {
			return "";
		} else {
			return title;
		}
	}
}
