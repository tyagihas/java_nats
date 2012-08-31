package org.nats.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import org.nats.*;

public class SubUnsub {

	public static void main(String[] args) throws Exception {
		
	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Session session = Session.connect(new Properties());
		session.start();

		System.out.println("Subscribing...");
		Integer sid = session.subscribe("hello", new MsgHandler() {
			public void execute(Object o) {
				System.out.println("Received update : " + (String)o);
			}
		});
		
		System.out.println("Press Enter to unsubscribe");
		bufferedReader.readLine();
		session.unsubscribe(sid);
		
		session.stop();
		System.exit(0);
	}
}
