package org.nats.examples;

import java.io.IOException;
import java.util.Properties;

import org.nats.*;

public class Request {

	public static void main(String[] args) throws Exception {
		final Connection conn1 = Connection.connect(new Properties());

		System.out.println("Subscribing...");		
		conn1.subscribe("help", new MsgHandler() {
			public void execute(String request, String replyTo) {
				try {
					conn1.publish(replyTo, "I can help!");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		conn1.flush();

		final Connection conn2 = Connection.connect(new Properties());
		conn2.request("help", new MsgHandler() {
			public void execute(String response) {
				System.out.println("Got a response for help : " + response);
				System.exit(0);
			}
		});
		conn2.flush();
	}
}
