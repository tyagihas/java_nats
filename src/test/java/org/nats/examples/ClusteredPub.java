package org.nats.examples;

import java.util.Properties;

import org.nats.Connection;

public class ClusteredPub {

	public static void main(String[] args) throws Exception {
		Properties opts = new Properties();
		// opts.put("dont_randomize_servers", Boolean.TRUE);
		opts.put("servers", "nats://user1:pass1@server1,nats://user1:pass1@server2:4243");
		Connection conn = Connection.connect(opts);

		System.out.println("Publishing...");		
		conn.publish("hello", "world");
		
		conn.close();
		System.exit(0);
	}
}
