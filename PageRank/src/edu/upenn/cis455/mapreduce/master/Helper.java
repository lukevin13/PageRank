package edu.upenn.cis455.mapreduce.master;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

public class Helper {

	public static String htmlStart = "<html><head></head><body>"
			+ "<h3><b>** Kevin Lu (lukevin) **</b></h3>";

	public static String htmlEnd = "</body></html>";

	// CSS for table style generated from http://www.tablesgenerator.com/html_tables
	private static String tableCSS = "<style type='text/css'> .tg  {border-collapse:collapse;border-spacing:2px;border-"
			+ "color:#777;width:50%;} .tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px"
			+ "5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal"
			+ ";border-color:#777;color:#444;background-color:#F7FDFA;} .tg th{font-"
			+ "family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px"
			+ ";border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-"
			+ "color:#777;color:#fff;background-color:#2777D6;} .tg th{font-weight:bold"
			+ ";vertical-align:top} .tg td{vertical-align:top;padding:10px} </style>";

	// Makes a table out of a list of worker statuses
	public static String generateStatusTable(List<WorkerStatus> workerStatusList) {
		String tableHTML = "";
		tableHTML += tableCSS;
		tableHTML += "<table class='tg'>"
				+ "<tr>"
				+ "<th>Worker ID</th>"
				+ "<th>IP:Port</th>"
				+ "<th>Status</th>"
				+ "</tr>";

		for (WorkerStatus ws : workerStatusList) {
			if (ws != null) {
				tableHTML += "<tr>"
						+ "<td>" + ws.getWorkerID() + "</td>"
						+ "<td>" + ws.getAddress() + "</td>"
						+ "<td>" + ws.getStatus() + "</td>"
						+ "</tr>";
			}
		}

		tableHTML += "</table>";
		return tableHTML;
	}

	// Makes buttons
	public static String generatePageRankButtons(List<WorkerStatus> ws) {
		String buttonHtml = ""
				+ "<form method='get' action='/status'><button type='submit'>Refresh</button></form>"
				+ "<form method='get' action='/runinit'><button type='submit'>Initialize</button></form>"
				+ "<form method='get' action='/runretrieve'><button type='submit'>Retrieve</button></form>"
				+ "<form method='get' action='/runconvert'><button type='submit'>Convert</button></form>"
				+ "<form method='get' action='/runmap'><button type='submit'>Map</button></form>"
				+ "<form method='get' action='/runshuffle'><button type='submit'>Shuffle</button></form>"
				+ "<form method='get' action='/runsort'><button type='submit'>Sort</button></form>"
				+ "<form method='get' action='/runreduce'><button type='submit'>Reduce</button></form>"
				+ "<form method='get' action='/runcollect'><button type='submit'>Collect</button></form>"
				+ "<form method='get' action='/runshare'><button type='submit'>Share</button></form>"
				+ "<form method='get' action='/updatedb'><button type='submit'>Update DB</button></form>";
		
		return buttonHtml;
	}

	// Checks if all members of a list are of a given value
	public static boolean checkSameStatus(List<WorkerStatus> ws, String status) {
		if (ws.isEmpty()) {
			return false;
		}
		boolean isSame = true;
		for (WorkerStatus w : ws) {
			if (w != null) {
				isSame &= w.isStatus(status);
			}
		}
		return isSame;
	}

	// Sends post to a worker
	public static void sendPost(String host, int port, String path, String body) {
		Socket s;
		try {
			s = new Socket(host, port);
			PrintWriter out = new PrintWriter(s.getOutputStream());
			out.println("POST " + path + " HTTP/1.1");
			out.println("Host: " + host + ":" + port);
			out.println("Content-Type: application/x-www-form-urlencoded");
			out.println("Content-Length: " + body.length());
			out.println("Connection: close");
			out.println();
			out.println(body);
			out.flush();
			out.close();
			s.close();
		} catch (IOException e) {
		}
	}
}
