package nats.examples;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Properties;

import nats.Client;

public class Pub {

	public static void main(String[] args) throws Exception {
		
		Client client = Client.connect(new Properties());
		client.start();

		System.out.println("Publishing...");
		client.publish("hello", "test", null, null);
		
		Thread.sleep(Long.MAX_VALUE);
	}
}
