package nats.examples;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Properties;

import nats.Client;
import nats.Client.EventHandler;

public class Sub {

	public static void main(String[] args) throws Exception {
		
		Client client = Client.connect(new Properties());
		client.start();

		System.out.println("Subscribing...");
		client.subscribe("hello", new EventHandler() {
			public void execute(Object o) {
				System.out.println("Received update : " + (String)o);
			}
		});
		
		Thread.sleep(Long.MAX_VALUE);
	}
}
