package nats.examples;

import java.util.Properties;
import nats.Session;

public class Pub {

	public static void main(String[] args) throws Exception {
		Session session = Session.connect(new Properties());
		session.start();

		System.out.println("Publishing...");		
		session.publish("hello", "world", null, null);
		session.flush();
		
		session.stop();
		System.exit(0);
	}
}
