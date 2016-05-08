package edu.upenn.cis455.mapreduce.worker;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.master.Helper;

public class WorkerServlet extends HttpServlet {
	private static final long serialVersionUID = -6176925620446518568L;

	// Servlet Parameters
	private String rootDir;
	String masterIP;
	int masterPort;
	String workerIP;
	int workerPort;

	// Worker attributes
	int workerID;
	String status;

	// Update Thread
	ScheduledExecutorService executor;
	WorkerUpdateThread wut;

	// RPC
	PageRank pagerank;

	@Override
	public void init(ServletConfig config) {
		// Get and store init parameters
		this.rootDir = config.getInitParameter("storagedir");
		String master = config.getInitParameter("master");
		String[] parsedMaster = master.split(":", 2);
		this.masterIP = parsedMaster[0];
		this.masterPort = Integer.parseInt(parsedMaster[1]);

		// Set initial status
		this.status = "Idle";

		// Create thread to be run every 10 seconds
		this.executor = Executors.newSingleThreadScheduledExecutor();
		this.wut = new WorkerUpdateThread(this);
		this.executor.scheduleAtFixedRate(this.wut, 0, 10, TimeUnit.SECONDS);

	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) {
		res.setContentType("text/html");
		res.setStatus(200);

		// Update worker address
		this.workerIP = req.getLocalAddr();
		this.workerPort = req.getLocalPort();

		try {
			// Prints out the information of the worker
			PrintWriter writer = res.getWriter();
			writer.println(Helper.htmlStart);
			writer.println("<p>Worker " + this.workerID + "</p>");
			writer.println("<p>Status " + this.status + "</p>");
			writer.println(Helper.htmlEnd);
			writer.println();
			writer.flush();
			writer.close();
		} catch (IOException e) {
		}
		
		this.wut.update();
	}


	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse res) {
		String path = req.getPathInfo();

		switch (path) {
		case ("/initialize") : {
			int numWorkers = Integer.parseInt(req.getParameter("numWorkers"));
			this.workerID = Integer.parseInt(req.getParameter("workerID"));
			String workerListString = req.getParameter("workerListString");
			List<String> workerList = makeWorkerList(workerListString);
			this.pagerank = new PageRank(this.rootDir, numWorkers, workerList);
			
			// Update worker address
			this.workerIP = req.getLocalAddr();
			this.workerPort = req.getLocalPort();
			
			this.status = "Initialized";
			break;
		}
		case ("/runCorpusTasks") : {
			String s3Files = req.getParameter("files");
			this.status = "Running Corpus Tasks";
			this.wut.update();
			this.pagerank.runCorpusTasks(s3Files);
			this.status = "Ready to Map";
			break;
		}
		case ("/runMap") : {
			this.status = "Mapping";
			this.wut.update();
			this.pagerank.runMap();
			this.status = "Map Complete";
			break;
		}
		case ("/runShuffleSort") : {
			this.status = "Shuffling and Sorting";
			this.wut.update();
			this.pagerank.runShuffleSort();
			this.status = "Ready to Reduce";
			break;
		}
		case ("/runReduce") : {
			this.status = "Reducing";
			this.wut.update();
			this.pagerank.runReduce();
			this.status = "Reduce Complete";
			break;
		}
		}
		this.wut.update();
	}

	// Makes an array of workers from a string
	private List<String> makeWorkerList(String workerListString) {
		List<String> list = new ArrayList<String>();
		String[] parsed = workerListString.split(",");
		for (String worker : parsed) {
			String[] parsed2 = worker.split("->", 2);
			int index = Integer.parseInt(parsed2[0]);
			list.add(index, parsed2[1]);
		}
		return list;
	}
}
