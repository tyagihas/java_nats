package nats.benchmark;

import java.util.Properties;
import nats.Session;
import nats.Session.EventHandler;

public class PubPerf {

	public static void main(String[] args) throws Exception {
		final int loop = 100000;
		int hash = 5000;
		
		Session session = Session.connect(new Properties());
		session.start();

		System.out.println("Performing Publish performance test");
		final long start = System.nanoTime();
		for(int i = 1; i <= loop; i++) {
			session.publish("hello", Integer.toString(i), null, null);
			if (i % hash == 0)
				System.out.print("+");
		}
		System.out.println("");
		
		session.flush(session.new EventHandler() {
			public void execute(Object o) {
				double elapsed = System.nanoTime() - start;
				System.out.println();
				System.out.println("elapsed : " + Double.toString(elapsed / 1000000000) + " seconds");
				System.out.println("msg/sec : " + Double.toString(loop / (elapsed / 1000000000)));	
			}
		});

		session.stop();		
		System.exit(0);
	}
}
