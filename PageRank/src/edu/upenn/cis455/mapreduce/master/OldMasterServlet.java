package edu.upenn.cis455.mapreduce.master;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class OldMasterServlet extends HttpServlet {

	private static final long serialVersionUID = 8522609402973199436L;

	private String root;
	private BDBWrapper bdb;
	private List<WorkerStatus> workerList = new ArrayList<WorkerStatus>();
	private String console = "/status";

	@Override
	public void init(ServletConfig config) {
//		String dirKey = "pagerank/storage/database/";
//		S3Wrapper s3 = new S3Wrapper();
		this.root = config.getInitParameter("storagedir");
//		File ranks = new File(this.root, "pagerank/database");
//		ranks = FileHelper.makeDirDeleteIfExists(ranks);
//
//		for (String filekey : s3.getFileList(dirKey)) {
//			File file = new File(this.root, filekey);
//			File dir = new File(file.getParentFile().getPath());
//			dir.mkdirs();
//			file = FileHelper.touchDeleteIfExists(file);
//			s3.download(filekey, file);
//		}
//
//		this.bdb = new BDBWrapper(this.root + "/pagerank/database");
//		this.bdb.close();
	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) {

		String path = req.getPathInfo();
		try {

			res.setContentType("text/html");
			PrintWriter writer = res.getWriter();

			switch (path) {
			case ("/status") : {

				// Returns a table of known workers and provides ability to run new jobs
				writer.println(Helper.htmlStart);
				writer.println(Helper.generateStatusTable(this.workerList));
				writer.println("</br>");
				writer.println(Helper.generatePageRankButtons(this.workerList));
				writer.println(Helper.htmlEnd);

				break;
			}
			case ("/switch") : {
				if (this.console.equals("/status")) {
					this.console = "/report";
				} else {
					this.console = "/status";
				}
				res.sendRedirect(this.console);
				break;
			}
			case ("/relay") : {
				String s = "<-->\r\n";
				for (WorkerStatus ws : this.workerList) {
					s += ws.getStatus() + "\r\n";
				}
				writer.println(s);
				break;
			}
			case ("/report") : {
				writer.println("Worker ID\tAddress\t\tStatus");
				for (WorkerStatus w : this.workerList) {
					writer.println(w.getWorkerID() + "\t\t" + w.getAddress() + "\t" + w.getStatus());
				}
				break;
			}
			case ("/runinit") : {
				String workerMapString = "";
				for (int i = 0; i < this.workerList.size(); i++) {
					workerMapString += (i + "->" + this.workerList.get(i).getAddress());
					if (i != this.workerList.size() - 1) {
						workerMapString += ",";
					}
				}

				for (int i = 0; i < this.workerList.size(); i++) {
					WorkerStatus w = this.workerList.get(i);
					String body = "numWorkers=" + this.workerList.size() + "&"
							+ "workerID=" + i + "&"
							+ "workerMapString=" + workerMapString;
					Helper.sendPost(w.getIP(), w.getPort(), "/initWorkers", body)	;
				}
				res.sendRedirect(this.console);
				break;
			}
			case ("/runretrieve") : {
				if (Helper.checkSameStatus(this.workerList, "initialized")) {
					S3Wrapper s3 = new S3Wrapper();
					List<String> filenames = new ArrayList<String>();
					String key = "doc_content/";
					for (String s : s3.getFileList(key)) {
						String name = s.replace(key, "");
						if (name != null && !name.isEmpty()) {
							filenames.add(name);
						}
					}
					for (int i = 0; i < this.workerList.size(); i++) {
						String body = "files=";
						for (int j = 0; j < filenames.size(); j++) {
							if (j % this.workerList.size() == i) {
								body += filenames.get(j) + ",";
							}
						}
						body = body.substring(0, body.length()-1);
						WorkerStatus w = this.workerList.get(i);
						Helper.sendPost(w.getIP(), w.getPort(), "/runretrieve", body);
					}
				}
				res.sendRedirect(this.console);
				break;
			}

			case ("/runconvert") : {

				if (Helper.checkSameStatus(this.workerList, "initialized") ||
						Helper.checkSameStatus(this.workerList, "retrieved")) {
					for (WorkerStatus w : this.workerList) {
						Helper.sendPost(w.getIP(), w.getPort(), "/runconvert", "");
					}
				}
				res.sendRedirect(this.console);
				break;
			}

			case ("/runmap") : {

				// Initiates map
				if (Helper.checkSameStatus(this.workerList, "ready to map") ||
					Helper.checkSameStatus(this.workerList, "initialized")) {
					for (WorkerStatus w : this.workerList) {
						Helper.sendPost(w.getIP(), w.getPort(), "/runmap", "");
					}
				}
				res.sendRedirect(this.console);

				break;
			}

			case ("/runshuffle") : {

				if (Helper.checkSameStatus(this.workerList, "ready to shuffle")) {
					for (WorkerStatus ws : this.workerList) {
						Helper.sendPost(ws.getIP(), ws.getPort(), "/runshuffle", "");
					}
				}
				res.sendRedirect(this.console);
				break;
			}

			case ("/runsort") : {

				if (Helper.checkSameStatus(this.workerList, "ready to sort")) {
					for (WorkerStatus ws : this.workerList) {
						Helper.sendPost(ws.getIP(), ws.getPort(), "/runsort", "");
					}
				}
				res.sendRedirect(this.console);
				break;
			}

			case ("/runreduce") : {

				// Initiate reduce phase
				if (Helper.checkSameStatus(this.workerList, "ready to reduce")) {
					for (WorkerStatus w : this.workerList) {
						Helper.sendPost(w.getIP(), w.getPort(), "/runreduce", "");
					}
				}
				res.sendRedirect(this.console);

				break;
			}

			case ("/runcollect") : {

				// Initiate reduce phase
				if (Helper.checkSameStatus(this.workerList, "complete")) {
					for (WorkerStatus w : this.workerList) {
						Helper.sendPost(w.getIP(), w.getPort(), "/runcollect", "");
					}
				}
				res.sendRedirect(this.console);

				break;
			}
			case ("/runshare") : {
				if (Helper.checkSameStatus(this.workerList, "collected")) {
					for (WorkerStatus w : this.workerList) {
						Helper.sendPost(w.getIP(), w.getPort(), "/runshare", "");
					}
				}
				res.sendRedirect(this.console);

				break;
			}

			case ("/updatedb") : {
				File dir = new File(this.root, "pagerank");
				dir = FileHelper.makeDirDeleteIfExists(dir);
				S3Wrapper s3 = new S3Wrapper();
				String key = "pagerank/storage/output/";

				for (String fstr : s3.getFileList(key)) {
					String nfile = fstr.replace(key, "");
					if (!nfile.isEmpty() && !nfile.contains("/")) {
						File file = new File(dir, nfile);
						file = FileHelper.touchDeleteIfExists(file);
						s3.download(key + nfile, file);
					}
				}

				this.bdb = new BDBWrapper(this.root + "/pagerank/database");

				for (File f : dir.listFiles()) {
					if (f.isFile()) {
						BufferedReader reader = new BufferedReader(new FileReader(f));
						String line = "";
						while ((line = reader.readLine()) != null) {
							if (!line.isEmpty()) {
								String pkey = line.split("\t", 2)[0];
								String[] parsed = pkey.split("::");
								this.bdb.putKeyValue("docID:pagerank", parsed[0], parsed[1]);
							}
						}
						reader.close();
					}
				}

				this.bdb.close();

				File bdbdir = new File(this.root + "/pagerank/database");
				String dbkey = "pagerank/storage/database/";
				if (bdbdir.exists() && bdbdir.isDirectory()) {
					for (File f : bdbdir.listFiles()) {
						if (f != null && f.isFile()) {
							s3.upload(dbkey + f.getName(), f);
						}
					}

				}
				res.sendRedirect(this.console);
				break;
			}

			default : {

				// Returns an info page
				writer.println(Helper.htmlStart);
				writer.println("<p>This the master servlet for page rank related tasks</p>");
				writer.println("<p>Available servlet paths:</p>");
				writer.println("<p>'GET /status' : Display the status of known workers and start new map/reduce jobs</p>");
				writer.println(Helper.htmlEnd);

			}
			}

			writer.flush();
			writer.close();

		} catch (IOException e) {
		}
	}

	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse res) {
		String path = req.getPathInfo();
		try {

			PrintWriter writer = res.getWriter();

			switch (path) {
			case ("/workerupdate") : {
				int workerID = Integer.parseInt(req.getParameter("workerID"));
				int workerPort = Integer.parseInt(req.getParameter("workerPort"));
				String workerHost = req.getRemoteAddr();
				String job = req.getParameter("job");
				String status = req.getParameter("status");

				WorkerStatus w = new WorkerStatus(workerID, workerPort, workerHost, job, status);
				if (this.workerList.contains(w)) {
					for (WorkerStatus ws : this.workerList) {
						if (ws.equals(w)) {
							ws.setWorkerID(workerID);
							ws.setStatus(status);
						}
					}
				} else {
					this.workerList.add(w);
				}
				res.sendRedirect("/status");
				break;
			}
			case ("/getRanks") : {
				String docIDs = req.getParameter("docIDs").trim();
				JSONArray rankList = new JSONArray();
				this.bdb = new BDBWrapper(this.root+"/pagerank/database");
				for (String encodeddocID : docIDs.split(",")) {
					String docID = URLDecoder.decode(encodeddocID, "UTF-8");
					String rank = this.bdb.getKeyValue("docID:pagerank", docID);
					JSONObject pageRankObj = new JSONObject();
					pageRankObj.put("docID",docID);
					if (rank != null) {
						pageRankObj.put("rank", rank);
					} else {
						pageRankObj.put("rank", "null");
					}
					rankList.add(pageRankObj);
				}
				this.bdb.close();

				rankList.writeJSONString(writer);
				break;
			}

			default : {

			}
			}
			writer.flush();
			writer.close();

		} catch (IOException e) {
		}
	}


}
