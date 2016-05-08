package edu.upenn.cis455.mapreduce.worker;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class WorkerUpdateThread implements Runnable {

	private WorkerServlet worker = null;

	public WorkerUpdateThread(WorkerServlet worker) {
		this.worker = worker;
	}

	@Override
	public void run() {
		update();
	}
	
	// Update method
	public void update() {
		try {
			Socket s = new Socket(this.worker.masterIP, this.worker.masterPort);
			String body = "workerID=" + this.worker.workerID + "&"
					+ "workerPort=" + this.worker.workerPort + "&"
					+ "status=" + this.worker.status;
			sendPost(s, "/workerupdate", body);
			s.close();
		} catch (UnknownHostException e) {
		} catch (IOException e) {
		}
	}

	// Sends post to a worker
	private void sendPost(Socket s, String path, String body) {
		try {
			PrintWriter out = new PrintWriter(s.getOutputStream());
			out.println("POST " + path + " HTTP/1.1");
			out.println("Host: " + this.worker.masterIP + ":" +  this.worker.masterPort);
			out.println("Content-Type: application/x-www-form-urlencoded");
			out.println("Content-Length: " + body.length());
			out.println("Connection: close");
			out.println();
			out.println(body);
			out.flush();
			out.close();
		} catch (IOException e) {
		}
	}
}
