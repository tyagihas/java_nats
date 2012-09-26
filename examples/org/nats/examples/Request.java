package org.nats.examples;

import java.io.IOException;
import java.util.Properties;

import org.nats.*;

public class Request {

	public static void main(String[] args) throws Exception {
		final Connection conn = Connection.connect(new Properties());
		conn.start();

		System.out.println("Subscribing...");		
		conn.subscribe("help", new MsgHandler() {
			public void execute(String request, String replyTo) {
				try {
					conn.publish(replyTo, "I can help!");
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
		});		

		System.out.println("Sending a request...");		
		Integer sid = conn.request("help", new MsgHandler() {
			public void execute(String response) {
				System.out.println("Got a response for help : " + response);
				System.exit(0);
			}
		});
	}
}
