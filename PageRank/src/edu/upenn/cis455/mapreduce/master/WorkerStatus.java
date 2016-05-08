package edu.upenn.cis455.mapreduce.master;

public class WorkerStatus {
	
	private int workerID;
	private int port;
	private String ip;
	private String job;
	private String status;
	private int numDoc;
	
	public WorkerStatus(int workerID, int port, String ip, String job, String status, int numDoc) {
		this.workerID = workerID;
		this.port = port;
		this.ip = ip;
		this.job = job;
		this.status = status;
		this.numDoc = numDoc;
	}
	
	public boolean isStatus(String status) {
		return this.status.equals(status);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof WorkerStatus)) {
			return false;
		}
		WorkerStatus ws = (WorkerStatus) obj;
		return (this.getAddress().equals(ws.getAddress()));
	}
	
	
	// Getters and Setters
	public String getAddress() {
		return this.ip + ":" + this.port;
	}

	public int getWorkerID() {
		return this.workerID;
	}

	public int getPort() {
		return this.port;
	}
	
	public String getIP() {
		return this.ip;
	}

	public String getJob() {
		return this.job;
	}

	public String getStatus() {
		return this.status;
	}

	public int getNumDoc() {
		return this.numDoc;
	}

	public void setWorkerID(int workerID) {
		this.workerID = workerID;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	public void setIP(String ip) {
		this.ip = ip;
	}

	public void setJob(String job) {
		this.job = job;
	}

	public void setStatus(String status) {
		this.status = status;
	}
	
	public void setNumDoc(int numDoc) {
		this.numDoc = numDoc;
	}
	
}
