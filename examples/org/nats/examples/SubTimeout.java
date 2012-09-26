package org.nats.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import org.nats.*;

public class SubTimeout {

	public static void main(String[] args) throws Exception {
	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Connection conn = Connection.connect(new Properties());
		conn.start();

		System.out.println("Listening on : " + args[0]);
		Integer sid = conn.subscribe(args[0], new MsgHandler() {
			int received = 0;
			public void execute() {
				received++;
			}
		});
		conn.timeout(sid, 1, null, new MsgHandler() {
			public void execute(Object o) {
				System.out.println("Timeout waiting for a message!");
			}
		});

		System.out.println("\nPress enter to exit.");
		bufferedReader.readLine();
		
		conn.flush();
		conn.stop();
		
		System.exit(0);
	}
}
