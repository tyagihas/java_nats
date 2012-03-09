package nats.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Properties;

import nats.Client;
import nats.Client.EventHandler;

public class SubUnsub {

	public static void main(String[] args) throws Exception {
		
	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		Client client = Client.connect(new Properties());
		client.start();

		System.out.println("Subscribing...");
		Integer sid = client.subscribe("hello", new EventHandler() {
			public void execute(Object o) {
				System.out.println("Received update : " + (String)o);
			}
		});
		
		System.out.println("Press Enter to unsubscribe");
		bufferedReader.readLine();
		client.unsubscribe(sid);
		
		Thread.sleep(Long.MAX_VALUE);
	}
}
