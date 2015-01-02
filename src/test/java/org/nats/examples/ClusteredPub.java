package org.nats.examples;

import java.util.Properties;

import org.nats.Connection;

public class ClusteredPub {

	public static void main(String[] args) throws Exception {
		Properties opts = new Properties();
		// opts.put("dont_randomize_servers", Boolean.TRUE);
		opts.put("servers", "nats://server1:4242,nats://server2:4243");
		opts.put("user", "user1");
		opts.put("pass", "pass1");
		Connection conn = Connection.connect(opts);

		System.out.println("Publishing...");		
		conn.publish("hello", "world");
		
		conn.close();
		System.exit(0);
	}
}
