package nats.benchmark;

import java.util.Properties;
import nats.Session;
import nats.Session.EventHandler;

public class PubSubPerf {

	public static void main(String[] args) throws Exception {
		final int loop = 100000;
		final int hash = 5000;
		
		final Session session1 = Session.connect(new Properties());
		session1.start();
		final Session session2 = Session.connect(new Properties());
		session2.start();

		System.out.println("Performing Publish/Subscribe performance test");
		
		final long start = System.nanoTime();
		session1.subscribe("test", session1.new EventHandler() {
			int received = 0;
			public void execute(Object o) {
				received++;
				if (received == loop) {
					double elapsed = System.nanoTime() - start;
					System.out.println();
					System.out.println("elapsed : " + Double.toString(elapsed / 1000000000) + " seconds");
					System.out.println("msg/sec : " + Double.toString(loop / (elapsed / 1000000000)));						

					System.out.println("Exiting...");
					System.exit(0);
				}
			}
		});
		
		session1.flush(session1.new EventHandler() {
			public void execute(Object o) {
				for(int i = 1; i <= loop; i++) {
					try {
						session2.publish("test", "a", null, null);
						if (i % hash == 0)
							System.out.print("+");
					}
					catch(Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
	}
}
