package org.nats.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import org.nats.*;

public class Sub {

	public static void main(String[] args) throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Connection conn = Connection.connect(new Properties());

		System.out.println("Listening on : " + args[0]);
		conn.subscribe(args[0], new MsgHandler() {
			public void execute(String msg) {
				System.out.println("Received update : " + msg);
			}
		});
		
		System.out.println("\nPress enter to exit.");
		bufferedReader.readLine();
		
		conn.close();
		System.exit(0);
	}
}
