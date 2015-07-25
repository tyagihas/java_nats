package org.nats.examples;

import java.util.Properties;

import org.nats.Connection;

public class Auth {

	public static void main(String[] args) throws Exception {
		Properties opts = new Properties();
		opts.setProperty("servers", "nats://teppei:T0pS3cr3t@localhost:4222");
		Connection conn = Connection.connect(opts);

		System.out.println("Publishing...");		
		conn.publish("hello", "world");
		
		conn.close();
		System.exit(0);
	}
}
