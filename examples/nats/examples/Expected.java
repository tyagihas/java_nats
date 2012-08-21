package nats.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;
import nats.Session;

public class Expected {

	public static void main(String[] args) throws Exception {
	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Session session = Session.connect(new Properties());
		session.start();

		System.out.println("Listening on : " + args[0]);
		System.out.println("Will timeout in 10 seconds unless 2 messages are received");
		Integer sid = session.subscribe(args[0], session.new EventHandler() {
			int received = 0;
			public void execute() {
				received++;
				System.out.println("Message received : " + received);
			}
		});
		Properties opt = new Properties();
		opt.put("expected", new Integer(2));
		session.timeout(sid, 10, opt, session.new EventHandler() {
			public void execute(Object o) {
				System.out.println("Timeout waiting for a message!");
			}
		});

		System.out.println("\nPress enter to exit.");
		bufferedReader.readLine();
		
		session.flush();
		session.stop();
		
		System.exit(0);
	}
}
