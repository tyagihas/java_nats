package org.nats.benchmark;

import java.io.IOException;
import java.util.Properties;

import org.nats.*;

public class PubSubPerf {

	public static void main(String[] args) throws Exception {
		final int loop = (args.length == 0 || args[0] == null) ? 100000 : Integer.parseInt(args[0]);
		int size = (args.length == 0 || args[1] == null) ? 1 : Integer.parseInt(args[1]);
		final int hash = 2500;
		
		StringBuffer buf = new StringBuffer();
		for(int l = 0; l < size; l++) buf.append("a");
		final String val = buf.toString();

		final Connection conn1 = Connection.connect(new Properties());
		final Connection conn2 = Connection.connect(new Properties());

		System.out.println("Performing Publish/Subscribe performance test");
		final long start = System.nanoTime();
		conn1.subscribe("test", new MsgHandler() {
			int received = 0;
			public void execute(Object o) {
				received++;
				// System.out.println("Execute : " + (String)o);
				if (received == loop) {
					double elapsed = System.nanoTime() - start;
					System.out.println();
					System.out.println("\nelapsed : " + Double.toString(elapsed / 1000000000) + " seconds");
					System.out.println("msg/sec : " + Double.toString(loop / (elapsed / 1000000000)));						

					System.out.println("Exiting...");
					System.exit(0);
				}
			}
		});
		
		conn1.flush(new MsgHandler() {
			public void execute(Object o) {
				try {
					for(int i = 1; i <= loop; i++) {
						conn2.publish("test", val);
						// conn2.publish("test", "aaaa\r\nbbbb\r\ncccc\r\ndddd\r\n");
						// conn2.publish("test", new Integer(i).toString());
						if (i % hash == 0)
							System.out.print("+");
					}
					conn2.flush();
				} catch(Exception e) {e.printStackTrace();}
			}
		});
	}
}
