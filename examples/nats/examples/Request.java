package nats.examples;

import java.io.IOException;
import java.util.Properties;
import nats.Session;
import nats.Session.EventHandler;

public class Request {

	public static void main(String[] args) throws Exception {
		final Session session = Session.connect(new Properties());
		session.start();

		System.out.println("Subscribing...");		
		session.subscribe("help", session.new EventHandler() {
			public void execute(String request, String replyTo) {
				try {
					session.publish(replyTo, "I can help!");
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
		});		

		System.out.println("Sending a request...");		
		Integer sid = session.request("help", session.new EventHandler() {
			public void execute(String response) {
				System.out.println("Got a response for help : " + response);
				System.exit(0);
			}
		});
		session.flush();			
	}
}
