/**
The MIT License (MIT)

Copyright (c) 2012-2016 Teppei Yagihashi

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

package org.nats;

import static org.nats.common.Constants.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.nats.common.NatsMonitor;
import org.nats.common.NatsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection represents a bidirectional channel to NATS server. Message handler may be attached to each operation
 * which is invoked when the operation is processed by the server. A client JVM may create multiple Connection objects
 * by calling {@link #connect(java.util.Properties popts)} multiple times.
 * @author Teppei Yagihashi
 */
public class Connection implements NatsMonitor.Resource {

	private Logger LOG = LoggerFactory.getLogger(Connection.class);
	
	private static int numConnections;
	private static volatile int ssid;

	private static ConcurrentHashMap<String, Connection> conns;
	private static NatsMonitor monitor = null;
    
	private Connection self;
	private MsgHandler connectHandler;
	private MsgHandler disconnectHandler;
	private String id;
	private MsgProcessor msgProc;
	private Thread processor;

	private Properties opts;
	private SocketChannel channel;
	private ByteBuffer sendBuffer;
	private int lastPos;
	private ByteBuffer receiveBuffer;
	private byte[] cmd = new byte[INIT_BUFFER_SIZE];
	
	private NatsUtil nUtil;
	
	private int status;
	private ConcurrentHashMap<Integer, Subscription> subs;    
	private ConcurrentLinkedQueue<MsgHandler> pongs;
	private Timer timer;
	private long lastOverflow;

	private volatile int connStatus;
	
	static {
		ssid = 1;
		numConnections = 0;
		conns = new ConcurrentHashMap<String, Connection>();
	}	
	
	/** 
	 * Create and return a Connection with various attributes.
	 * @param popts Properties object containing connection attributes
	 * @return newly created Connection object
	 */
	public static Connection connect(Properties popts) throws IOException, InterruptedException {
		return Connection.connect(popts, null);
	}
	
	/** 
	 * Create and return a Connection with various attributes. connectHandler is invoked when connected to a server.
	 * @param popts Properties object containing connection attributes
	 * @param connectHandler MsgHandler to be invoked when connection is established
	 * @return newly created Connection object
	 */
	public static Connection connect(Properties popts, MsgHandler connectHandler) throws IOException, InterruptedException {
		init(popts);
		return Connection.connect(popts, connectHandler, null);
	}

	/** 
	 * Create and return a Connection with various attributes.
	 * connectHandler is invoked when connected to a server and disconnectHandler is invoked when disconnected.
	 * @param popts Properties object containing connection attributes
	 * @param connectHandler MsgHandler to be invoked when connection is established
	 * @param disconnectHandler MsgHandler to be invoked when connection is disconnected and unable to reach any server
	 * @return newly created Connection object
	 */
	public static Connection connect(Properties popts, MsgHandler connectHandler, MsgHandler disconnectHandler) throws IOException, InterruptedException {
		init(popts);
		return new Connection(popts, connectHandler, disconnectHandler);
	}
	
	protected static void init(Properties popts) {
		// Defaults
		if (!popts.containsKey("verbose")) popts.put("verbose", Boolean.FALSE);
		if (!popts.containsKey("pedantic")) popts.put("pedantic", Boolean.FALSE);
		if (!popts.containsKey("reconnect")) popts.put("reconnect", Boolean.TRUE);
		if (!popts.containsKey("ssl")) popts.put("ssl", new Boolean(false));
		if (!popts.containsKey("max_reconnect_attempts")) popts.put("max_reconnect_attempts", new Integer(DEFAULT_MAX_RECONNECT_ATTEMPTS));
		if (!popts.containsKey("reconnect_time_wait")) popts.put("reconnect_time_wait", new Integer(DEFAULT_RECONNECT_TIME_WAIT));
		if (!popts.containsKey("ping_interval")) popts.put("ping_interval", new Integer(DEFAULT_PING_INTERVAL));
		if (!popts.containsKey("dont_randomize_servers")) popts.put("dont_randomize_servers", Boolean.FALSE);

		// Overriding with ENV
		if (System.getenv("NATS_URI") != null) popts.put("uri", System.getenv("NATS_URI")); else if (!popts.containsKey("uri")) popts.put("uri", DEFAULT_URI);
		if (System.getenv("NATS_URIS") != null) popts.put("uris", System.getenv("NATS_URIS"));
		if (System.getenv("NATS_VERBOSE") != null) popts.put("verbose", new Boolean(System.getenv("NATS_VERBOSE")));
		if (System.getenv("NATS_PEDANTIC") != null) popts.put("pedantic", new Boolean(System.getenv("NATS_PEDANTIC")));
		if (System.getenv("NATS_RECONNECT") != null) popts.put("reconnect", new Boolean(System.getenv("NATS_RECONNECT")));
		if (System.getenv("NATS_SSL") != null) popts.put("ssl", new Boolean(System.getenv("NATS_SSL")));
		if (System.getenv("NATS_MAX_RECONNECT_ATTEMPTS") != null) popts.put("max_reconnect_attempts", Integer.parseInt(System.getenv("NATS_MAX_RECONNECT_ATTEMPTS")));
		if (System.getenv("NATS_MAX_RECONNECT_TIME_WAIT") != null) popts.put("max_reconnect_time_wait", Integer.parseInt(System.getenv("NATS_MAX_RECONNECT_TIME_WAIT")));	
		if (System.getenv("NATS_PING_INTERVAL") != null) popts.put("ping_interval", Integer.parseInt(System.getenv("NATS_PING_INTERVAL")));
		if (System.getenv("NATS_DONT_RANDOMIZE_SERVERS") != null) popts.put("dont_randomize_servers", Boolean.parseBoolean(System.getenv("NATS_DONT_RANDOMIZE_SERVERS")));
	}

	protected Connection(Properties popts, MsgHandler connectHandler, MsgHandler disconnectHandler) throws IOException, InterruptedException {
		id = Integer.toString(numConnections++);
		self = this;
		msgProc = new MsgProcessor();
		processor = new Thread(msgProc, "NATS_Processor-" + id);
		sendBuffer = ByteBuffer.allocateDirect(INIT_BUFFER_SIZE);
		lastPos = 0;
		receiveBuffer = ByteBuffer.allocateDirect(INIT_BUFFER_SIZE);
		status = AWAITING_CONTROL;
		subs = new ConcurrentHashMap<Integer, Subscription>();
		pongs = new ConcurrentLinkedQueue<MsgHandler>();
		nUtil = new NatsUtil();

		opts = popts;
		Server.addServers(opts);
		timer = new Timer("NATS_Timer-" + id);

		if (!connect()) {
			Server server = Server.current();
			throw new IOException("Failed connecting to " + server.getHost() + ":" + server.getPort());
		}
		
		if (connectHandler != null) { this.connectHandler = connectHandler; }
		if (disconnectHandler != null) { this.disconnectHandler = disconnectHandler; }

		processor.setDaemon(true);
		processor.start();
		sendConnectCommand();
	}
	
	@Override
	public String getResourceId() {
		return id;
	}
	
	private boolean connect() {
		try {
			Server server = Server.current();
			InetSocketAddress addr = new InetSocketAddress(server.getHost(), server.getPort());
			channel = SocketChannel.open(addr);
			while(!channel.isConnected()){}			
			server.setConnected(true);
			connStatus = OPEN;
			conns.put(this.toString(), this);
			// Activating monitor if not running
			if (monitor == null) {
				monitor = NatsMonitor.getInstance();
				monitor.start();
			}
			monitor.addResource(id, this);
		} catch(Exception ie) {
			return false;
		}

		return true;
	}

	private String hexRand(int limit, Random rand) {
		return Integer.toHexString(rand.nextInt(limit));
	}

	/**
	 * Create a properly formatted inbox subject.
	 * @return String value representing inbox subject
	 */
	public String createInbox()
	{
		Random rand = new Random();
		return "_INBOX_" + hexRand(0x0010000, rand) 
				+ hexRand(0x0010000, rand)
				+ hexRand(0x0010000, rand)
				+ hexRand(0x0010000, rand)
				+ hexRand(0x0010000, rand)
				+ hexRand(0x0100000, rand);
	}

	private void sendConnectCommand() throws IOException {
		Server server = Server.current();
		String user = null;
		String pass = null;
		StringBuffer sb = new StringBuffer("CONNECT {\"verbose\":");
		sb.append(((Boolean)opts.get("verbose")).toString());
		sb.append(",\"pedantic\":").append((Boolean)opts.get("pedantic"));
		if (opts.get("user") != null) {
			user = (String)opts.getProperty("user");
			pass = (String)opts.getProperty("pass");
		}
		if (server.getUser() != null) {
			user = server.getUser();
			pass = server.getPass();			
		}

		if (user != null) sb.append(",\"user\":\"").append(user).append("\"");
		if (pass != null) sb.append(",\"pass\":\"").append(pass).append("\"");
		sb.append("}").append(CR_LF);
		
		sendCommand(sb.toString());
	}

	
	/**
	 * Close the channel with flushing buffer and stop the background thread.
	 * @throws IOException
	 */
	public void close() throws IOException {
		close(true);
	}
	
	/**
	 * Close the channel and stop the background thread.
	 * @param flush flushing messages in buffer before closing
	 * @throws IOException
	 */
	public void close(boolean flush) throws IOException {
		if (flush)
			flush();
		connStatus = CLOSE;
		processor.interrupt();
		channel.close();
		conns.remove(this.toString());
		// Deactivating pinger if no connection is available.
		if (conns.size() == 0) {
			monitor.interrupt();
			monitor = null;
		}
		timer.cancel();
	}

	/**
	 * Return status (true or false) of a connection to the server.
	 * @return connection status
	 */
	public boolean isConnected() {
		return channel.isConnected();
	}

	/**
	 * Publish a text message to the given subject.
	 * @param subject
	 * @param msg a message to be delivered to the server
	 * @throws IOException
	 */
	public void publish(String subject, String msg) throws IOException {
		publish(subject, null, msg, null);
	}

	/**
	 * Publish a text message to the given subject.
	 * @param subject
	 * @param msg a message to be delivered to the server
	 * @param handler event handler is invoked when publish has been processed by the server.
	 * @throws IOException
	 */
	public void publish(String subject, String msg, MsgHandler handler) throws IOException {
		publish(subject, null, msg, handler);
	}

	/**
	 * Publish a text message to the given subject, with optional reply and event handler.
	 * @param subject
	 * @param opt_reply
	 * @param msg a message to be delivered to the server
	 * @param handler event handler is invoked when publish has been processed by the server.
	 * @throws IOException
	 */
	public void publish(String subject, String opt_reply, String msg, MsgHandler handler) throws IOException {
		publish(subject, opt_reply, msg.getBytes(), handler);
	}

	/**
	 * Publish a binary message to the given subject.
	 * @param subject
	 * @param msg a message to be delivered to the server
	 * @throws IOException
	 */
	public void publish(String subject, byte[] msg) throws IOException {
		publish(subject, null, msg, null);
	}

	/**
	 * Publish a binary message to the given subject.
	 * @param subject
	 * @param msg a message to be delivered to the server
	 * @param handler event handler is invoked when publish has been processed by the server.
	 * @throws IOException
	 */	
	public void publish(String subject, byte[] msg, MsgHandler handler) throws IOException {
		publish(subject, null, msg, handler);
	}

	/**
	 * Publish a binary message to the given subject, with optional reply and event handler.
	 * @param subject
	 * @param opt_reply
	 * @param msg a message to be delivered to the server
	 * @param handler event handler is invoked when publish has been processed by the server.
	 * @throws IOException
	 */	
	public void publish(String subject, String opt_reply, byte[] msg, MsgHandler handler) throws IOException {
		if (subject == null) return;

		int offset = bytesCopy(cmd, 0, "PUB ");
		offset = bytesCopy(cmd, offset, subject);
		cmd[offset++] = 0x20; // SPC
		if (opt_reply != null)  {
			offset = bytesCopy(cmd, offset, opt_reply);
			cmd[offset++] = 0x20;
		}
		int length = msg.length;
		offset = bytesCopy(cmd, offset, Integer.toString(length));
		cmd[offset++] = 0xd; // CRLF
		cmd[offset++] = 0xa;
		System.arraycopy(msg, 0, cmd, offset, length);
		offset += length;
		cmd[offset++] = 0xd; // CRLF
		cmd[offset++] = 0xa;

		sendCommand(cmd, offset, false);

		if (handler != null) sendPing(handler);	
	}

	private int bytesCopy(byte[] b, int start, String data) {
		int end = start + data.length();
		for(int idx = 0; start < end; start++, idx++)
			b[start] = (byte)data.charAt(idx);

		return end;
	}
	
	/**
	 * Subscribe to a subject with optional wildcards. Messages will be delivered to the supplied callback.
	 * @param subject optionally with wildcards
	 * @param handler event handler is invoked when a message is delivered
	 * @return subscription ID
	 * @throws IOException
	 */
	public Integer subscribe(String subject, MsgHandler handler) throws IOException {
		return this.subscribe(subject, null, handler);
	}

	/**
	 * Subscribe to a subject with optional wildcards. Messages will be delivered to the supplied callback.
	 * @param subject optionally with wildcards
	 * @param popts optional option properties, e.g. queue, max
	 * @param handler event handler is invoked when a message is delivered
	 * @return subscription ID
	 * @throws IOException
	 */
	public Integer subscribe(String subject, Properties popts, MsgHandler handler) throws IOException {
		Integer sid = ssid++;
		Subscription sub = new Subscription(sid, subject, handler);

		if (popts != null) {
			sub.queue = (String)popts.getProperty("queue");
			sub.max = (popts.getProperty("max") == null ? -1 : Integer.parseInt(popts.getProperty("max")));
		}

		subs.put(sid, sub);
		sendSubscription(subject, sid, sub);

		return sid;
	}

	private void sendSubscription(String subject, Integer sid, Subscription sub) throws IOException {
		sendCommand("SUB " + subject + SPC + (sub.queue == null ? "" : sub.queue + SPC) + sid.toString() + CR_LF);

		if (sub.max != -1) this.unsubscribe(sid, sub.max);
	}

	private void sendSubscriptions() throws IOException {
		Entry<Integer, Subscription> entry = null;

		for(Iterator<Entry<Integer, Connection.Subscription>> iter = subs.entrySet().iterator(); iter.hasNext();) {
			entry = iter.next();
			sendSubscription(entry.getValue().subject, entry.getKey(), entry.getValue());
		}		
	}

	/**
	 * Cancel a subscription.
	 * @param sid
	 * @throws IOException
	 */
	public void unsubscribe(Integer sid) throws IOException {
		this.unsubscribe(sid, 0);
	}

	/**
	 * Cancel a subscription.
	 * @param sid 
	 * @param opt_max optional number of responses to receive before auto-unsubscribing
	 * @throws IOException
	 */
	public void unsubscribe(Integer sid, int opt_max) throws IOException {
		Subscription sub = subs.get(sid);
		if (sub == null) return;
		if (opt_max < 0) opt_max = 0;
    	    	
		sendCommand("UNSUB " + sid.toString() + SPC + Integer.toString(opt_max) + CR_LF);
    	
		if (sub.received >= opt_max) subs.remove(sid);
	}
    
	/**
	 * Return the active subscription count.
	 * @return subscription count
	 */
	public int getSubscriptionCount() {
		return subs.size();
	}
    
	private void sendCommand(String command) throws IOException {
		sendCommand(command.getBytes(), command.length(), false);
	}
    
	private synchronized void sendCommand(byte[] data, int length, boolean priority) throws IOException {
		if ((connStatus != OPEN) || !isConnected()) {
			throw new IOException("Connection is disconnected");
		}

		while (true) {
			try {
				sendBuffer.put(data, 0, length);
				if (sendBuffer.position() <= length) {
					// Flushing very first message
					timer.schedule(new TimerTask() {
						public void run() {
							try {
								flushPending();
							} catch (IOException e) {
								LOG.error(e.getMessage() + ", Failed flushing messages");
							}
						}
					}, 1);
					return;
				}
				if (priority || (sendBuffer.position() > MAX_PENDING_SIZE)) { flushPending(); }
				break;
			} catch(BufferOverflowException bofe) {
				flushPending();
				// Reallocating send buffer if bufferoverflow occurs too frequently
				if (sendBuffer.capacity() < MAX_BUFFER_SIZE) {
					if ((System.currentTimeMillis() - lastOverflow) < REALLOCATION_THRESHOLD) {
						LOG.debug("Expanding sendBuffer from " + sendBuffer.capacity() + " to " + sendBuffer.capacity()*2);
						LOG.debug("Copying " + sendBuffer.limit() + " bytes");
						sendBuffer = nUtil.expandBuffer(sendBuffer);
					}
					lastOverflow = System.currentTimeMillis();
				}
			}
		}		
	}
	
	private synchronized void flushPending() throws IOException {
		if ((lastPos = sendBuffer.position()) > 0) {
			try {
				sendBuffer.flip();
				for(;;) {
					if (sendBuffer.position() == sendBuffer.limit()) break;
					channel.write(sendBuffer);
				}
				sendBuffer.clear();
			} catch (IOException ie) {
				if (connStatus == OPEN) {
					connStatus = RECONNECT;
					reconnect();
				}
			}
		}
	}
	
	/**
	 * Ping a server and invoke a handler when PONG is returned.
	 * @param handler
	 * @throws IOException
	 */
	public void sendPing(MsgHandler handler) throws IOException {
		synchronized(pongs) {
			pongs.add(handler);
		}
		sendCommand(PING_REQUEST, PING_REQUEST_LEN, true);
	}
    
	/**
	 * Send a request and have the response delivered to the supplied handler.
	 * @param subject
	 * @param handler
	 * @return subscription ID
	 * @throws IOException
	 */
	public Integer request(String subject, MsgHandler handler) throws IOException {
		return request(subject, EMPTY, null, handler);
	}

	/**
	 * Send a request and have the response delivered to the supplied handler.
	 * @param subject
	 * @param msg
	 * @param popts
	 * @param handler
	 * @return subscription ID
	 * @throws IOException
	 */
	public Integer request(String subject, String msg, Properties popts, MsgHandler handler) throws IOException {
		return request(subject, msg, null, popts, handler);
	}

	/**
	 * Send a request and have the response delivered to the supplied handler.
	 * @param subject
	 * @param msg
	 * @param popts
	 * @param handler
	 * @return subscription ID
	 * @throws IOException
	 */
	public Integer request(String subject, byte[] msg, Properties popts, MsgHandler handler) throws IOException {
		return request(subject, null, msg, popts, handler);
	}
	
	private Integer request(String subject, String msg, byte[] bmsg, Properties popts, MsgHandler handler) throws IOException {
		if (subject == null) return null;
    	
		String inbox = createInbox();
		Integer sub = subscribe(inbox, popts, handler);
		if (msg != null) { publish(subject, inbox, msg, null); }
		
		return sub;
	}
    
	/**
	 * Flush buffered messages with no event handler.
	 */
	public void flush() throws IOException {
		flush(new MsgHandler() {});
	}

	/**
	 * Flush buffered messages with a specified event handler.
	 * @param handler is invoked when flush is completed.
	 */
	public void flush(MsgHandler handler) throws IOException {
		handler.caller = Thread.currentThread();
		sendPing(handler);
		try {
			handler.caller.join();
		} catch (InterruptedException e) {}
	}
    
	/**
	 * Return version String.
	 * @return version String
	 */
	public String getVersion() {
		return "<nats java " + version + ">";
	}
    
	/**
	 * Set a timeout on a subscription.
	 * @param sid subscription ID
	 * @param tout
	 * @param prop
	 * @param handler
	 */
	public void timeout(final Integer sid, long tout, Properties prop, final MsgHandler handler) {
		Subscription sub = subs.get(sid);
		if (sub == null) return;
		
		boolean au = false;
		if (prop != null) {
			au = (prop.get("auto_unsubscribe") == null ? false : ((Boolean)prop.get("auto_unsubscribe")).booleanValue());
			sub.expected = (prop.get("expected") == null ? -1 : ((Integer)prop.get("expected")).intValue());
		}
		final boolean auto_unsubscribe = au;
		
		if (sub.task != null) sub.task.cancel();
		
		final Connection parent = this;
		TimerTask task = new TimerTask() {
			public void run() {
				try {
					if (auto_unsubscribe) { parent.unsubscribe(sid); }
				} catch(IOException e) {
					LOG.error(e.getMessage() + ", TimerTask failed unsubscribing " + sid);
				}
				
				if (handler != null) handler.execute(sid);
			}
		}; 
		timer.schedule(task, tout * 1000);
		sub.task = task;
	}
	
	private synchronized void reconnect() throws IOException {
		boolean doReconnect = ((Boolean)opts.get("reconnect")).booleanValue();

		if (doReconnect) {
			int max_reconnect_attempts = ((Integer)opts.get("max_reconnect_attempts")).intValue();
			int reconnect_time_wait = ((Integer)opts.get("reconnect_time_wait")).intValue();
			// Salvaging unsent messages in sendBuffer
			byte[] unsent = null;
			if ((lastPos = sendBuffer.position()) > 0) {
				unsent = new byte[lastPos]; 
				try {
					sendBuffer.flip();
					sendBuffer.get(unsent, 0, lastPos);
				} catch(BufferUnderflowException e) {
					LOG.error(e.getMessage() + ", Failed reading unsent messages from sendBuffer");
				}
				sendBuffer.clear();
				lastPos = 0;
			}

			conns.remove(this.toString());
			for(int i = 0; i < max_reconnect_attempts; i++) {
				try {
					channel.close();
					connect();

					if (isConnected()) {
						receiveBuffer.clear();
						status = AWAITING_CONTROL;
						sendConnectCommand();
						sendSubscriptions();
						if (unsent != null) { sendCommand(unsent, unsent.length, false); }
						flushPending();
						connStatus = OPEN;
						break;
					}	
					Thread.sleep(reconnect_time_wait);
					Server.next();
				} catch(IOException ie) {
					Server server = Server.current();
					LOG.warn(ie.getMessage() + ", Failed connecting to " + server.getHost() + ":" + server.getPort());
					continue;
				} catch (InterruptedException e) {
					LOG.error(e.getMessage() + ", Reconnecting process is interruped");
				}
			}

			// Failing reconnection to all servers
			if (connStatus == RECONNECT) {
				if (disconnectHandler != null) { disconnectHandler.execute(); }
				throw new IOException("Failed connecting to all servers");
			}
		}
		processor = new Thread(msgProc, "NATS_Processor-" + id);
		processor.setDaemon(true);
		processor.start();
	}
	
	private class ReconnectTask extends TimerTask {
		private final Logger LOG = LoggerFactory.getLogger(ReconnectTask.class);
		
		@Override
		public void run() {
			if (processor.isAlive())
				processor.interrupt();
			try {
				reconnect();
			} catch (IOException e) {
				LOG.error(e.getMessage() + ", Reconnecting process has failed");
			}
		}
	}
	
	/**
	 * Internal data structure to hold subscription meta data
	 * @author Teppei Yagihashi
	 */
	public class Subscription {
		private Integer sid = null;
		private String subject = null;
		private MsgHandler handler = null;
		private String queue = null;
		private int max = -1;
		private int received = 0;
		private int expected = -1;
		private TimerTask task = null;
    	
		public Subscription(Integer psid, String psubject, MsgHandler phandler) {
			sid = psid;
			subject = psubject;
			handler = phandler;
		}
	}
	
	/**
	 * Main thread for processing incoming and outgoing messages
	 * @author Teppei Yagihashi
	 */
	private final class MsgProcessor implements Runnable {
		private int pos;
		private String subject;
		private String optReply;
		private int payload_length;
		private Subscription sub;
		
		private long lastTruncated;
		private boolean reallocate;

		public MsgProcessor() {
			pos = 0;
			payload_length = -1;
			reallocate = false;
		}
		
		public void run() {
			for(;;) {
				try {
					processMessage();
				} catch(AsynchronousCloseException ace) {
					continue;
				} catch (InterruptedException ie) {
					break;
				} catch (IOException e) {
					// skipping if reconnect already starts due to -ERR code
					if (connStatus == OPEN) {
						connStatus = RECONNECT;
						timer.schedule(new ReconnectTask(), 1);
					}
					break;
				}
			}
		}    	
		
		/**
		 * Parse and process various incoming messages
		 */
		private void processMessage() throws IOException, InterruptedException {
			if (channel.read(receiveBuffer) > 0) {
				receiveBuffer.flip();
				while (true) {
					if (receiveBuffer.position() >= receiveBuffer.limit()) { break; }
					
					switch(status) {
					case AWAITING_CONTROL :
						if ((pos = nUtil.readNextOp(pos, receiveBuffer)) == 0) {
							if (nUtil.compare(MSG, 3)) {
								status = AWAITING_MSG_PAYLOAD;
								parseMsg();
								if (receiveBuffer.limit() < (payload_length + receiveBuffer.position() + 2)) {
									// Don't clear() yet and keep it in buffer
									receiveBuffer.compact();
									reallocate = verifyTruncation();
									return;									
								}
								break;
							}
							else if (nUtil.compare(PONG, 4)) {
								MsgHandler handler;
								synchronized(pongs) { 
									handler = pongs.poll(); 
								}
								processEvent(handler);
								if (handler.caller != null) { 
									handler.caller.interrupt(); 
								}
							}
							else if (nUtil.compare(PING, 4)) { sendCommand(PONG_RESPONSE, PONG_RESPONSE_LEN, false); }
							else if (nUtil.compare(ERR, 4)) {
								if (connStatus == OPEN) {
									connStatus = RECONNECT;
									timer.schedule(new ReconnectTask(), 1);
								}
							}
							else if (nUtil.compare(OK, 3)) {/* do nothing for now */}
							else if (nUtil.compare(INFO, 4)) {
								Server.current().parseServerInfo(nUtil.getOp());
								if (connectHandler != null) { connectHandler.execute((Object)self); }
							}
						}
						else { lastTruncated = System.currentTimeMillis(); }
						
						break;
					case AWAITING_MSG_PAYLOAD :
						receiveBuffer.get(nUtil.getBuffer(), 0, payload_length + 2);
						on_msg(); 
						status = AWAITING_CONTROL;
						break;
					}
				}
				receiveBuffer.clear();
				
				// Reallocation occurs only when receiveBuffer is empty.
				if (reallocate) {
					receiveBuffer = ByteBuffer.allocateDirect(receiveBuffer.capacity() * 2);
					reallocate = false;
				}
			}
		}
		
		// Reallocating receive buffer if message truncation occurs too frequently
		private boolean verifyTruncation() {
			boolean result = false;
			if (receiveBuffer.capacity() < MAX_BUFFER_SIZE) {	
				if (System.currentTimeMillis() - lastTruncated < REALLOCATION_THRESHOLD)
					result = true;
			}
			lastTruncated = System.currentTimeMillis();
			
			return result;
		}
		
		private void on_msg() throws IOException {
			if (sub != null) {
				sub.received++;
				if (sub.max != -1) {
					if (sub.max < sub.received)
						return;
					else if (sub.max == sub.received) 
						subs.remove(sub.sid);
				}
				processEvent(sub.handler);

				if ((sub.task != null) && (sub.received >= sub.expected)) {
					sub.task.cancel();
					sub.task = null;
				}
			}
		}
		
		private String getString() {
			return new String(nUtil.getBuffer(), 0, payload_length);
		}
		
		private byte[] getByteArray() {
			byte[] arr = new byte[payload_length];
			System.arraycopy(nUtil.getBuffer(), 0, arr, 0, payload_length);

			return arr;
		}
		
		private void processEvent(MsgHandler handler) throws IOException {
			switch(handler.arity) {
			case 0 :
				handler.execute();
				break;
			case 1 :
				handler.execute(getString());
				break;
			case 2 :
				handler.execute(getString(), optReply);
				optReply = null;
				break;
			case 3 :
				handler.execute(getString(), optReply, subject);
				optReply = null;
				subject = null;
				break;
			case -1 :
				handler.execute((Object)getString());
				break;
			case 11 :
				handler.execute(getByteArray());
				break;
			case 12 :
				handler.execute(getByteArray(), optReply);
				optReply = null;
				break;
			case 13 :
				handler.execute(getByteArray(), optReply, subject);
				optReply = null;
				subject = null;
				break;
			}
		}
		
		// Extracting MSG parameters
		private void parseMsg() {
			int index = 0, rid = 0, start = 0;
			byte[] buf = nUtil.getBuffer();
			for( ; buf[index++] != 0xd; ) {
				if (buf[index] == 0x20) {
					if (rid == 1)
						subject = new String(new String(buf,start,index-start));						
					else if (rid == 2) 
						sub = subs.get(Integer.valueOf(new String(buf,start,index-start)));
					else if (rid == 3)
						optReply = new String(new String(buf,start,index-start));
					rid++;
					start = ++index;
				}
			}
			payload_length = Integer.parseInt(new String(buf,start,--index-start));
		}		
	}
}
