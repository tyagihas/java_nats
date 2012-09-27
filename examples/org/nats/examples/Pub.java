package org.nats.examples;

import java.util.Properties;

import org.nats.Connection;

public class Pub {

	public static void main(String[] args) throws Exception {
		Connection conn = Connection.connect(new Properties());

		System.out.println("Publishing...");		
		conn.publish("hello", "world");
		
		conn.close();
		System.exit(0);
	}
}
