package org.nats.examples;

import java.util.Properties;

import org.nats.Connection;

public class Pub {

	public static void main(String[] args) throws Exception {
		Connection conn = Connection.connect(new Properties());
		conn.start();

		System.out.println("Publishing...");		
		conn.publish("hello", "world");
		conn.flush();
		
		Thread.sleep(Long.MAX_VALUE);
		
		conn.stop();
		System.exit(0);
	}
}
