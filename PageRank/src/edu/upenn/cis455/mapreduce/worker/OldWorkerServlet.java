package edu.upenn.cis455.mapreduce.worker;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.master.Helper;

public class OldWorkerServlet extends HttpServlet {

	private static final long serialVersionUID = 1338699252390722268L;

	// Init parameters
	String storageDir = "/storage";
	String masterIP = "localhost";
	int masterPort = 8080;

	// Worker properties
	int numWorkers = 0;
	int workerID = 0;
	String workerIP;
	int workerPort = 8080;
	String job = "none";
	String status = "new";
	Map<Integer, String> workerMap = new HashMap<Integer, String>();

	ScheduledExecutorService executor;
	WorkerUpdateThread wut;
	PageRank pr;

	@Override
	public void init(ServletConfig config) {

		// Get init parameters
		String ipPort = config.getInitParameter("master");
		String[] spl = ipPort.split(":", 2);

		this.storageDir = config.getInitParameter("storagedir");
		this.masterIP = spl[0];
		this.masterPort = Integer.parseInt(spl[1]);

		// Create thread to be run every 10 seconds
		this.executor = Executors.newSingleThreadScheduledExecutor();
//		this.wut = new WorkerUpdateThread(this);
		this.executor.scheduleAtFixedRate(this.wut, 0, 10, TimeUnit.SECONDS);

	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) {
		this.workerPort = req.getServerPort();

		String path = req.getPathInfo();
		try {

			res.setContentType("text/html");
			PrintWriter writer = res.getWriter();

			switch (path) {
			case ("/status") : {
				writer.println(Helper.htmlStart);
				writer.println("<p><b>Current Status of Worker</b></p>");
				writer.println("<p>Master Host: " + this.masterIP + "</p>");
				writer.println("<p>Master Port: " + this.masterPort + "</p>");
				writer.println("<p>Worker ID: " + this.workerID + "</p>");
				writer.println("<p>Worker Port: " + this.workerPort + "</p>");
				writer.println("<p>Job: " + this.job + "</p>");
				writer.println("<p>Status: " + this.status + "</p>");
				break;
			}

			default : {
				// Returns an info page
				writer.println(Helper.htmlStart);
				writer.println("<p>This a worker servlet for page rank related tasks</p>");
				writer.println("<p>Available servlet paths:</p>");
				writer.println("<p>'GET /status' : Display the status of known workers</p>");
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
		System.out.println("Received: " + path);

		switch (path) {
		case ("/initWorkers") : {
			if (this.status.equals("new")) {
				int workerID = Integer.parseInt(req.getParameter("workerID"));
				int numWorkers = Integer.parseInt(req.getParameter("numWorkers"));
				String workerMapString = req.getParameter("workerMapString");
//				String bdbDir = req.getParameter("bdbDir");
				this.workerMap = processWorkerMap(workerMapString);
				this.workerIP = req.getLocalAddr();
				this.workerPort = req.getLocalPort();
				this.workerID = workerID;
				this.numWorkers = numWorkers;
//				this.pr = new PageRank(this.storageDir, this.numWorkers, this.workerID, this.workerMap);
				this.status = "initialized";
				this.wut.update();
			}
			break;
		}
		case ("/runconvert") : {
			this.status = "converting";
			this.wut.update();

			// CONVERT
//			this.pr.convertPhase();

			this.status = "ready to map";
			this.wut.update();

			break;
		}
		case ("/runmap") : {
			if (this.status.equals("ready to map") || this.status.equals("initialized")) {
				this.status = "mapping";
				this.wut.update();

				// MAP

//				this.pr.mapPhase();

				this.status = "ready to shuffle";
				this.wut.update();
			}

			break;
		}
		case ("/runshuffle") : {
			if (this.status.equals("ready to shuffle")) {
				this.status = "shuffling";
				this.wut.update();

				// SHUFFLE
//				this.pr.shufflePhase();

				this.status = "ready to sort";
				this.wut.update();
			}
			break;
		}
		case ("/runretrieve") : {
			if (this.status.equals("initialized")) {
				this.status = "retrieving the corpus";
				this.wut.update();
				
				String files = req.getParameter("files");
//				this.pr.retrievePhase(files);
				
				this.status = "retrieved";
				this.wut.update();
			}

			break;
		}
	
		case ("/runreduce") : {
			if (this.status.equals("ready to reduce")) {
				this.status = "reducing";
				this.wut.update();

				// REDUCE
//				this.pr.reducePhase();

				this.status = "complete";
				this.wut.update();
			}

			break;
		}
		case ("/runcollect") : {
			if (this.status.equals("complete")) {
				this.status = "collecting";
				this.wut.update();

				// COLLECT
//				this.pr.collectPhase();

				this.status = "collected";
				this.wut.update();
			}
			break;
		}
		case ("/runsort") : {
			this.status = "sorting";
			this.wut.update();

//			this.pr.sortPhase();

			this.status = "ready to reduce";
			this.wut.update();
			break;
		}
		case ("/runshare") : {
			this.status = "sharing";
			this.wut.update();
//			this.pr.share();

			this.status = "ready to map";
			this.wut.update();
			break;
		}

		default : {

		}
		}
	}

	// Creates Map from workerMapString
	private Map<Integer, String> processWorkerMap(String workerMapString) {
		Map<Integer, String> wm = new HashMap<Integer, String>();
		String[] parsed = workerMapString.split(",");
		for (String line : parsed) {
			String[] parsedLine = line.split("->");
			int id = Integer.parseInt(parsedLine[0]);
			String address = parsedLine[1];
			wm.put(id, address);
		}
		return wm;
	}
}
