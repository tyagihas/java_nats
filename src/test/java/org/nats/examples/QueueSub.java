package org.nats.examples;

import java.util.Properties;
import org.nats.*;

public class QueueSub {

	public static void main(String[] args) throws Exception {
		Connection conn1 = Connection.connect(new Properties());
		Connection conn2 = Connection.connect(new Properties());

		System.out.println("Listening on : " + args[0]);
		Properties opt = new Properties();
		opt.setProperty("queue", "job.workers");
		
		conn1.subscribe(args[0], opt, new MsgHandler() {
			public void execute(String msg) {
				System.out.println("conn1 Received update : " + msg);
			}
		});
		conn1.flush();

		conn2.subscribe(args[0], opt, new MsgHandler() {
			public void execute(String msg) {
				System.out.println("conn2 Received update : " + msg);
			}
		});
		conn2.flush();
		
		for(int i = 0; i < 10; i++) {
			conn1.publish(args[0], "message " + i);
		}
		conn1.flush();
		
		conn1.close();
		conn2.close();
		System.exit(0);
	}
}
