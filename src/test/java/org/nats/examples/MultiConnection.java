package org.nats.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.nats.*;

public class MultiConnection {

	public static void main(String[] args) throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Connection conn1 = Connection.connect(new Properties());

		conn1.subscribe("test", new MsgHandler() {
			public void execute(String msg, String reply, String subject) {
				System.out.println("Received update on " + subject + " : " + msg);
			}
		});

		Connection conn2 = Connection.connect(new Properties(), new MsgHandler() {
			public void execute(Object o) {
				Connection conn = (Connection)o;
				try {
					conn.publish("test", "Hello World!");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		
		System.out.println("\nPress enter to exit.");
		bufferedReader.readLine();
		
		conn1.close();
		conn2.close();
		System.exit(0);
	}
}
