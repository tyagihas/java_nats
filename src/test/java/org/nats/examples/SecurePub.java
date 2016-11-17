package org.nats.examples;

import java.util.Properties;

import org.nats.Connection;

public class SecurePub {

	public static void main(String[] args) throws Exception {
		// For debugging purpose
		// System.setProperty("javax.net.debug", "all");
		// System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");

		Properties props = new Properties();
		props.put("keystore", "./keystore");
		props.put("keystore_pass", "password");
		props.put("truststore", "./truststore");
		props.put("truststore_pass", "password");
		Connection conn = Connection.connect(props);

		System.out.println("Publishing...");		
		conn.publish("hello", "world");

		conn.close();
		System.exit(0);
	}
}
