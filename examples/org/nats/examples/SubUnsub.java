package org.nats.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import org.nats.*;

public class SubUnsub {

	public static void main(String[] args) throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Connection conn = Connection.connect(new Properties());

		System.out.println("Subscribing...");
		Integer sid = conn.subscribe("hello", new MsgHandler() {
			public void execute(Object o) {
				System.out.println("Received update : " + (String)o);
			}
		});
		
		System.out.println("Press Enter to unsubscribe");
		bufferedReader.readLine();
		conn.unsubscribe(sid);
		
		conn.close();
		System.exit(0);
	}
}
