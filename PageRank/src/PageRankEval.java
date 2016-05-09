import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class PageRankEval {

	private static String ip = "52.202.180.149";
	private static String lp = "localhost";
	private static int port = 8084;

	public static void main(String args[]) {

		if (args.length != 1) {
			System.exit(0);
		}

		int run = Integer.parseInt(args[0]);

		Socket socket;
		long startTime = System.currentTimeMillis();
		try {

			for (int i = 0; i < run; i++) {
				String status = "Ready to Map";

				System.out.println("** ITERATION: " + (i+1));

				boolean flag = true;
				boolean shared = false;
				while (!shared) {
					flag = true;

					socket = new Socket(ip, port);
					if (socket.isClosed()) {
						Thread.sleep(5000);
						continue;
					}

					PrintWriter writer = new PrintWriter(socket.getOutputStream());
					BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					sendRelayGet(writer, "/relay");

					String line = "";

					while (line != null) {
						line = reader.readLine();
						if (line.equals("<-->")) {
							break;
						}
					}

					while ((line = reader.readLine()) != null) {
						if (line != null && !line.isEmpty()) {
							System.out.println(status + " = " + line);
							flag &= (line.equals(status));
							System.out.println(flag);
						}
					}
					socket.close();

					if (!flag) {
						Thread.sleep(5000);
						continue;
					}
					System.out.println("Time for " + run + " iterations" + (System.currentTimeMillis() - startTime));



					switch (status) {
					case ("Ready to Map") : {
						status = "Map Complete";
						sendGet("/runMap");
						break;
					}
					case ("Map Complete") : {
						status = "Ready to Reduce";
						sendGet("/runShuffleSort");
						break;
					}
					case ("Ready to Reduce") : {
						status = "Reduce Complete";
						sendGet("/runReduce");
						break;
					}
					case ("Reduce Complete") : {
						status = "Ready to Map";
						System.out.println("Done reduce");
						shared = true;
						System.out.println("Time for " + run + " iterations" + (System.currentTimeMillis() - startTime));

						break;
					}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Time for " + run + " iterations" + (System.currentTimeMillis() - startTime));
	}

	public static void sendGet(String path) {
		try {
			Socket socket = new Socket(ip, port);
			PrintWriter writer = new PrintWriter(socket.getOutputStream());
			writer.println("GET " + path + " HTTP/1.1");
			writer.println("Host: localhost:8084");
			writer.println("Connection: close");
			writer.println("");
			writer.flush();
			writer.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void sendRelayGet(PrintWriter writer, String path) {
		writer.println("GET " + path + " HTTP/1.1");
		writer.println("Host: localhost:8084");
		writer.println("Connection: close");
		writer.println("");
		writer.flush();
	}
}
