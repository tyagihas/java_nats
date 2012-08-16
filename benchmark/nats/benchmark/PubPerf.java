package nats.benchmark;

import java.util.Properties;
import nats.Session;
import nats.Session.EventHandler;

public class PubPerf {

	public static void main(String[] args) throws Exception {
		final int loop = (args.length == 0 || args[0] == null) ? 100000 : Integer.parseInt(args[0]);
		int size = (args.length == 0 || args[1] == null) ? 1 : Integer.parseInt(args[1]);
		final int hash = 2500;
		String val = "";
		for(int l = 0; l < size; l++) val+="a";
		
		Session session = Session.connect(new Properties());
		session.start();

		System.out.println("Performing Publish performance test");
		final long start = System.nanoTime();
		for(int i = 1; i <= loop; i++) {
			// session.publish("hello", null, new Integer(i).toString(), null);
			session.publish("hello", null, val, null);
			if (i % hash == 0)
				System.out.print("+");
		}
		
		session.flush(session.new EventHandler() {
			public void execute(Object o) {
				double elapsed = System.nanoTime() - start;
				System.out.println("\nelapsed : " + Double.toString(elapsed / 1000000000) + " seconds");
				System.out.println("msg/sec : " + Double.toString(loop / (elapsed / 1000000000)));	
			}
		});

		session.stop();		
		System.exit(0);
	}
}
