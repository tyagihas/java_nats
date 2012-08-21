package nats.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import nats.Session;
import nats.Session.EventHandler;

public class MultiConnection {

	public static void main(String[] args) throws Exception {
	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Session session1 = Session.connect(new Properties());
		session1.start();

		session1.subscribe("test", session1.new EventHandler() {
			public void execute(String msg, String reply, String subject) {
				System.out.println("Received update on " + subject + " : " + msg);
			}
		});

		Session session2 = Session.connect(new Properties());
		session2.start(session2.new EventHandler() {
			public void execute(Object o) {
				Session session = (Session)o;
				try {
					session.publish("test", "Hello World!");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		
		System.out.println("\nPress enter to exit.");
		bufferedReader.readLine();

		session1.flush();
		session1.stop();
		session2.flush();
		session2.stop();
		System.exit(0);
	}
}
