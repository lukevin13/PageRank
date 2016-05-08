package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class MasterServlet extends HttpServlet {

	private static final long serialVersionUID = -872616717245905981L;

	// Servlet Parameters
	String rootDir;
	private List<WorkerStatus> workerList = new ArrayList<WorkerStatus>();

	// Database and S3
	private String s3CorpusDirKey = "test_docs/";
	private String s3DatabaseDirKey = "pagerank/storage/database/";
	private String s3OutputDirKey = "pagerank/storage/output/";
	private File localDatabase;
	private File outputDir;

	@Override
	public void init(ServletConfig config) {
		this.rootDir = config.getInitParameter("storagedir");

		// Pull down the current database from S3
		this.localDatabase = new File(this.rootDir, "database");
		this.outputDir = new File(this.rootDir, "output");
		FileHelper.makeDirDeleteIfExists(this.localDatabase);
		FileHelper.makeDirDeleteIfExists(this.outputDir);

		S3Wrapper s3 = new S3Wrapper();
		for (String s3FileKey : s3.getFileList(this.s3DatabaseDirKey)) {
			String s3Filename = s3FileKey.replace(this.s3DatabaseDirKey, "");
			if (!s3Filename.isEmpty()) {
				File localFile = new File(this.localDatabase, s3Filename);
				FileHelper.touchDeleteIfExists(localFile);
				s3.download(s3FileKey, localFile);
			}
		}
	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) {
		String path = req.getPathInfo();
		res.setContentType("text/html");

		switch (path) {
		case ("/status") : {
			try {
				PrintWriter writer = res.getWriter();
				writer.println(Helper.htmlStart);
				writer.println(Helper.generateStatusTable(this.workerList));				
				writer.println(Helper.htmlEnd);
				writer.flush();
				writer.close();
			} catch (IOException e) {
			}
			break;
		}
		case ("/relay") : {
			try {
				String wsList = "<-->\r\n";
				for (WorkerStatus ws : this.workerList) {
					wsList += ws.getStatus() + "\r\n";
				}
				PrintWriter writer = res.getWriter();
				writer.println(wsList);
				writer.flush();
				writer.close();
			} catch (IOException e) {
			}
			try {
				res.sendRedirect("/status");
			} catch (IOException e) {
			}
			break;
		}
		case ("/initialize") : {
			String workerListString = "";
			for (int i = 0; i < this.workerList.size(); i++) {
				workerListString += i + "->" + this.workerList.get(i).getAddress() + ",";
			}
			workerListString = workerListString.substring(0, workerListString.length()-1);

			for (int i = 0; i < this.workerList.size(); i++) {
				String body = "workerID=" + i + "&"
						+ "numWorkers=" + this.workerList.size() + "&"
						+ "workerListString=" + workerListString;
				Helper.sendPost(this.workerList.get(i).getIP(), this.workerList.get(i).getPort(), "/initialize", body);
			}
			
			try {
				res.sendRedirect("/status");
			} catch (IOException e) {
			}

			break;
		}
		case ("/runCorpusTasks") : {
			S3Wrapper s3 = new S3Wrapper();
			List<String> s3Filekeys = new ArrayList<String>();
			for (String s : s3.getFileList(this.s3CorpusDirKey)) {
				String name = s.replace(this.s3CorpusDirKey, "");
				if (name != null && !name.isEmpty()) {
					s3Filekeys.add(name);
					System.out.println(name);
				}
			}
	
			for (int i = 0; i < this.workerList.size(); i++) {
				String body = "files=";
				for (int j = 0; j < s3Filekeys.size(); j++) {
					if (j % this.workerList.size() == i) {
						body += s3Filekeys.get(j) + ",";
					}
				}
				body = body.substring(0, body.length()-1);
				WorkerStatus w = this.workerList.get(i);
				Helper.sendPost(w.getIP(), w.getPort(), "/runCorpusTasks", body);
			}
			try {
				res.sendRedirect("/status");
			} catch (IOException e) {
			}
			break;
		}
		case ("/runMap") : {
			for (WorkerStatus ws: this.workerList) {
				Helper.sendPost(ws.getIP(), ws.getPort(), "/runMap", "");
			}
			try {
				res.sendRedirect("/status");
			} catch (IOException e) {
			}
			break;
		}
		case ("/runShuffleSort") : {
			for (WorkerStatus ws: this.workerList) {
				Helper.sendPost(ws.getIP(), ws.getPort(), "/runShuffleSort", "");
			}
			try {
				res.sendRedirect("/status");
			} catch (IOException e) {
			}
			break;
		}
		case ("/runReduce") : {
			for (WorkerStatus ws: this.workerList) {
				Helper.sendPost(ws.getIP(), ws.getPort(), "/runReduce", "");
			}
			try {
				res.sendRedirect("/status");
			} catch (IOException e) {
			}
			break;
		}
		case ("/updateDatabase") : {

			// Download output files from s3
			S3Wrapper s3 = new S3Wrapper();
			for (String s3FileKey : s3.getFileList(this.s3OutputDirKey)) {
				String s3Filename = s3FileKey.replace(this.s3OutputDirKey, "");
				if (!s3Filename.isEmpty()) {
					File localfile = new File(this.outputDir, s3Filename);
					FileHelper.touchDeleteIfExists(localfile);
					s3.download(s3FileKey, localfile);
				}
			}

			// Read the output files and update the database
			BDBWrapper bdb = new BDBWrapper(this.localDatabase.getAbsolutePath());
			for (File file : this.outputDir.listFiles()) {
				try {
					BufferedReader reader = new BufferedReader(new FileReader(file));
					String line = "";
					while ((line = reader.readLine()) != null) {
						if (!line.isEmpty()) {
							String[] parsed = line.split("\t", 2);
							String[] parsed2 = parsed[0].split(":");
							String page = parsed2[0];
							String rank = parsed2[1];
							bdb.putKeyValue("docID:pagerank", page, rank);
						}
					}
					reader.close();
				} catch (FileNotFoundException e) {
				} catch (IOException e) {
				}
			}
			bdb.close();

			// Upload the database to S3
			s3.delete(this.s3DatabaseDirKey);
			for (File localfile : this.localDatabase.listFiles()) {
				if (localfile.isFile()) {
					String s3Key = this.s3DatabaseDirKey + localfile.getName();
					s3.upload(s3Key, localfile);
				}
			}
			break;
		}
		default : {
			try {
				res.sendRedirect("/status");
			} catch (IOException e) {
			}
		}
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
				BDBWrapper bdb = new BDBWrapper(this.localDatabase.getAbsolutePath());
				for (String encodeddocID : docIDs.split(",")) {
					String docID = encodeddocID;
					String rank = bdb.getKeyValue("docID:pagerank", docID);
					JSONObject pageRankObj = new JSONObject();
					pageRankObj.put("docID", docID);
					if (rank != null) {
						pageRankObj.put("rank", rank);
					} else {
						pageRankObj.put("rank", "null");
					}
					rankList.add(pageRankObj);
				}
				bdb.close();

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
