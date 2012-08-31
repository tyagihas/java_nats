package org.nats.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import org.nats.*;

public class AutoUnsub {

	public static void main(String[] args) throws Exception {
	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Session session = Session.connect(new Properties());
		session.start();

		System.out.println("Listening on : " + args[0] + ", auto unsubscribing after 5 messages, but will send 10");
		Properties opt = new Properties();
		opt.setProperty("max", "5");
		session.subscribe(args[0], opt, new MsgHandler() {
			public void execute(String msg) {
				System.out.println("Received update : " + msg);
			}
		});
		
		for(int i = 0; i < 10; i++)
			session.publish(args[0], Integer.toString(i));
		
		System.out.println("\nPress enter to exit.");
		bufferedReader.readLine();
		
		session.flush();
		session.stop();
		
		System.exit(0);
	}
}
