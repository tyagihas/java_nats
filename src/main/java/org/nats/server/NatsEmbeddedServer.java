/**
The MIT License (MIT)

Copyright (c) 2012-2020 Teppei Yagihashi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
**/

package org.nats.server;

import static org.nats.common.Constants.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.nats.common.NatsMonitor;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NatsEmbeddedServer is compatible with NATS protocol and emulates behavior of 
 * <a href="http://nats.io/documentation/server/gnatsd-intro/">gnatsd server</a>. It currently does
 * not support SSL/TSL and clustering features.
 * @author Teppei Yagihashi
 */
public class NatsEmbeddedServer extends Thread {
	private Logger LOG = Logger.getLogger(NatsEmbeddedServer.class.getName());
	
	private NatsMonitor monitor;

	private static Properties opts;
	private ConcurrentHashMap<String, ClientConnection> conns;

	private int clientCounter;
	private int status;

	public static Object get(String key) {
		return opts.get(key);
	}
	
	/** 
	 * Create an instance of NatsEmbeddedServer, with various properties. 
	 * @param popts Properties object containing port, auth, ssl and max_payload parameters
	 */
	public NatsEmbeddedServer(Properties popts) {
		conns = new ConcurrentHashMap<String, ClientConnection>();

		if (!popts.containsKey("port")) { popts.put("port", Integer.toString(DEFAULT_PORT)); }
		if (!popts.containsKey("auth")) { popts.put("auth", new Boolean(DEFAULT_AUTH)); }
		if (!popts.containsKey("ssl")) { popts.put("ssl", new Boolean(DEFAULT_SSL)); }
		if (!popts.containsKey("max_payload")) { popts.put("max_payload", new Integer(DEFAULT_MAX_PAYLOAD)); }

		if (System.getenv("NATS_PORT") != null) { popts.put("port", Integer.parseInt(System.getenv("NATS_PORT"))); }
		if (System.getenv("NATS_AUTH") != null) { popts.put("auth", Boolean.parseBoolean(System.getenv("NATS_AUTH"))); }
		if (System.getenv("NATS_SSL") != null) { popts.put("ssl", Boolean.parseBoolean(System.getenv("NATS_SSL"))); }
		if (System.getenv("NATS_MAX_PAYLOAD") != null) { popts.put("max_payload", Integer.parseInt(System.getenv("NATS_MAX_PAYLOAD"))); }
		
		opts = popts;
		clientCounter = 0;

		this.start();
		monitor = NatsMonitor.getInstance();
		monitor.start();
	}
	
	private void setStatus(int status) {
		this.status = status;
	}

	/** 
	 * Shutdown NatsEmbeddedServer. 
	 */	
	public void shutdown() {
		this.setStatus(SHUTDOWN);
		this.interrupt();
	}

	@Override
	public void run() {
		ServerSocketChannel serverSocketChannel = null;
		SocketChannel channel = null;
		ClientConnection conn = null;
		Socket sock = null;
		int port = Integer.parseInt(opts.getProperty("port"));
		LOG.log(Level.ALL, "Starting NatsEmbeddedServer");
		try {
			serverSocketChannel =  ServerSocketChannel.open();
			serverSocketChannel.bind(new InetSocketAddress(port));
			this.setStatus(START);
		} catch(IOException e) {
			LOG.log(Level.SEVERE, e.getMessage() + ", Failed binding port=" + port);
			System.exit(1);
		}
		
		for(;;) {
			try {
				channel = serverSocketChannel.accept();
				sock = channel.socket();
				sock.setTcpNoDelay(true);
				String clientId = Integer.toString(++clientCounter);
				conn = new ClientConnection(clientId, channel);
				conns.put(clientId, conn);
				conn.start();
				monitor.addResource(clientId, conn);
			} catch (IOException e) {
				LOG.log(Level.SEVERE, e.getMessage() + ", Error accepting a socket");
			}
		}
	}
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("port", "4222");
		NatsEmbeddedServer server = new NatsEmbeddedServer(props);

		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			server.LOG.info("Server exiting...");
		}
	}
}
