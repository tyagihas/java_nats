package org.nats.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import org.nats.*;

public class QueueSub {

	public static void main(String[] args) throws Exception {
	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Connection conn = Connection.connect(new Properties());
		conn.start();

		System.out.println("Listening on : " + args[0]);
		Properties opt = new Properties();
		opt.setProperty("queue", "job.workers");
		conn.subscribe(args[0], opt, new MsgHandler() {
			public void execute(String msg) {
				System.out.println("Received update : " + msg);
			}
		});
		
		System.out.println("\nPress enter to exit.");
		bufferedReader.readLine();
		
		conn.flush();
		conn.stop();
		
		System.exit(0);
	}
}
