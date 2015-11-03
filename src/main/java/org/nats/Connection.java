package org.nats;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection represents a bidirectional channel to NATS server. Message handler may be attached to each operation
 * which is invoked when the operation is processed by the server. A client JVM may create multiple Connection objects
 * by calling {@link #connect(java.util.Properties popts)} multiple times.
 * 
 * @author Teppei Yagihashi
 *
 */
public class Connection {

	private static final String version = "0.5.2";
	
	private Logger LOG = LoggerFactory.getLogger(Connection.class);
	
	public static final int DEFAULT_PORT = 4222;
	public static final String DEFAULT_URI = "nats://localhost:" + Integer.toString(DEFAULT_PORT);

	// Parser state
	private static final int AWAITING_CONTROL = 0;
	private static final int AWAITING_MSG_PAYLOAD = 1;
	// Connection state
	private static final int OPEN = 0;
	private static final int CLOSE = 1;
	private static final int RECONNECT = 2;

	// Reconnect Parameters, 2 sec wait, 10 tries
	public static final int DEFAULT_RECONNECT_TIME_WAIT = 2*1000;
	public static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 3;
	public static final int DEFAULT_PING_INTERVAL = 4*1000;
	
	public static final int MAX_PENDING_SIZE = 32768;
	public static final int INIT_BUFFER_SIZE = 1 * 1024 * 1024; // 1 Mb
	public static final int MAX_BUFFER_SIZE = 16 * 1024 * 1024; // 16 Mb
	public static final long REALLOCATION_THRESHOLD = 1 * 1000; // 1 second
	
	private static final String CR_LF = "\r\n";
	private static final int CR_LF_LEN = CR_LF.length();
	protected static final String EMPTY = "";
	private static final String SPC = " ";

	// Protocol
	private static final byte[] PUB = "PUB".getBytes();
	private static final byte[] SUB = "SUB".getBytes();
	private static final byte[] UNSUB = "UNSUB".getBytes();
	private static final byte[] CONNECT = "CONNECT".getBytes();
	private static final byte[] MSG = "MSG".getBytes();
	private static final byte[] PONG = "PONG".getBytes();
	private static final byte[] PING = "PING".getBytes();
	private static final byte[] INFO = "INFO".getBytes();
	private static final byte[] ERR = "-ERR".getBytes();
	private static final byte[] OK = "+OK".getBytes();

	// Responses
	private static final byte[] PING_REQUEST = ("PING" + CR_LF).getBytes();
	private static final int PING_REQUEST_LEN = PING_REQUEST.length;
	private static final byte[] PONG_RESPONSE = ("PONG" + CR_LF).getBytes();
	private static final int PONG_RESPONSE_LEN = PONG_RESPONSE.length;

	private static int numConnections;
	private static volatile int ssid;

	private class Server {
		public String host;
		public int port;
		public String user;
		public String pass;
		public boolean connected = false;
		public int reconnect_attempts = 0;
	}
	private Server[] servers;
	private int current;
	
	private static ConcurrentHashMap<String, Connection> conns;
	private static Pinger pinger = null;
    
	private Connection self;
	private MsgHandler connectHandler;
	private int id;
	private MsgProcessor msgProc;
	private Thread processor;

	private Properties opts;
	private SocketChannel channel;
	private ByteBuffer sendBuffer;
	private int lastPos;
	private ByteBuffer receiveBuffer;
	private byte[] cmd = new byte[INIT_BUFFER_SIZE];
	
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
	
	private static class Pinger extends Thread {
		private static final Logger LOG = LoggerFactory.getLogger(Pinger.class);
		public Pinger() {
			super("pinger");
			setDaemon(true);
		}

		@Override
		public void run() {
			MsgHandler pingHandler = new MsgHandler() {};
			int ping_interval = (System.getenv("NATS_PING_INTERVAL") == null) ? DEFAULT_PING_INTERVAL : Integer.parseInt(System.getenv("NATS_PING_INTERVAL"));		
			for(;;) {
				try {
					Thread.sleep(ping_interval);
					for(Enumeration<Connection> e = conns.elements(); e.hasMoreElements();) {
						Connection conn = e.nextElement();
						if ((conn != null) && (conn.isConnected()))
							conn.sendPing(pingHandler);
					}
				} catch(IOException ie) {
					LOG.error(ie.getMessage());
					break;
				} catch (InterruptedException e) {
					LOG.info(e.getMessage());
					break;
				}
			}			
		}
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
	 * Create and return a Connection with various attributes. 
	 * @param popts Properties object containing connection attributes
	 * @param handler MsgHandler to be invoked when connection is established
	 * @return newly created Connection object
	 */
	public static Connection connect(Properties popts, MsgHandler handler) throws IOException, InterruptedException {
		init(popts);
		return new Connection(popts, handler);
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

	protected Connection(Properties popts, MsgHandler handler) throws IOException, InterruptedException {
		id = numConnections++;
		self = this;
		msgProc = new MsgProcessor();
		processor = new Thread(msgProc, "NATS_Processor-" + id);
		sendBuffer = ByteBuffer.allocateDirect(INIT_BUFFER_SIZE);
		lastPos = 0;
		receiveBuffer = ByteBuffer.allocateDirect(INIT_BUFFER_SIZE);
		status = AWAITING_CONTROL;
		subs = new ConcurrentHashMap<Integer, Subscription>();
		pongs = new ConcurrentLinkedQueue<MsgHandler>();

		opts = popts;
		configServers();
		timer = new Timer("NATS_Timer-" + id);
		current = 0;

		if (!connect())
			throw new IOException("Failed connecting to " + servers[current].host + ":" + servers[current].port);
		
		if (handler != null)
			connectHandler = handler;
		processor.setDaemon(true);
		processor.start();
		sendConnectCommand();
	}
	
	private void configServers() {
		String[] serverStrings = null;
		if (opts.containsKey("uris")) 
			serverStrings = ((String)opts.get("uris")).split(",");
		else if (opts.containsKey("servers")) 
			serverStrings = ((String)opts.get("servers")).split(",");
		else if (opts.containsKey("uri"))
			serverStrings = ((String)opts.get("uri")).split(",");

		servers = new Server[serverStrings.length];
		Random rand = (((Boolean)opts.get("dont_randomize_servers")) == Boolean.TRUE ? null : new Random());
		String[] uri;
		for(int i = 0; i < serverStrings.length; i++) {
			int idx = i;
			for(;rand != null;) {
				idx = rand.nextInt(servers.length);
				if (servers[idx] == null) break;
			}
			uri = serverStrings[i].split(":");
			servers[idx] = new Server();
			if (serverStrings[i].contains("@")) {
				servers[idx].user = uri[1].substring(2, uri[1].length());
				servers[idx].pass = uri[2].split("@")[0];				
				servers[idx].host = uri[2].split("@")[1];
				servers[idx].port = Integer.parseInt(uri[3]);
			}
			else {
				servers[idx].user = null;
				servers[idx].pass = null;
				servers[idx].host = uri[1].substring(2, uri[1].length());
				servers[idx].port = Integer.parseInt(uri[2]);
			}
		}
	}
	
	private boolean connect() {
		try {
			InetSocketAddress addr = new InetSocketAddress(servers[current].host, servers[current].port);
			channel = SocketChannel.open(addr);
			while(!channel.isConnected()){}			
			servers[current].connected = true;
			connStatus = OPEN;
			conns.put(this.toString(), this);
			// Activating pinger if not running
			if (pinger == null) {
				pinger = new Pinger();
				pinger.start();			
			}
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
		return "_INBOX." + hexRand(0x0010000, rand) 
				+ hexRand(0x0010000, rand)
				+ hexRand(0x0010000, rand)
				+ hexRand(0x0010000, rand)
				+ hexRand(0x0010000, rand)
				+ hexRand(0x0100000, rand);
	}

	private void sendConnectCommand() throws IOException {
		String user = null;
		String pass = null;
		StringBuffer sb = new StringBuffer("CONNECT {\"verbose\":");
		sb.append(((Boolean)opts.get("verbose")).toString());
		sb.append(",\"pedantic\":").append((Boolean)opts.get("pedantic"));
		if (opts.get("user") != null) {
			user = (String)opts.getProperty("user");
			pass = (String)opts.getProperty("pass");
		}
		if (servers[current].user != null) {
			user = servers[current].user;
			pass = servers[current].pass;			
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
			pinger.interrupt();
			pinger = null;
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
			sub.queue = (popts.getProperty("queue") == null ? " " : (String)popts.getProperty("queue"));
			sub.max = (popts.getProperty("max") == null ? -1 : Integer.parseInt(popts.getProperty("max")));
		}

		subs.put(sid, sub);
		sendSubscription(subject, sid, sub);

		return sid;
	}

	private void sendSubscription(String subject, Integer sid, Subscription sub) throws IOException {
		sendCommand("SUB " + subject + SPC + sub.queue + SPC + sid.toString() + CR_LF);

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
    
	private void sendCommand(byte[] data, int length, boolean priority) throws IOException {
		for(;;) {
			try {
				synchronized(sendBuffer) {
					sendBuffer.put(data, 0, length);
					if (sendBuffer.position() <= length) {
						// Flushing very first message
						timer.schedule(new TimerTask() {
							public void run() {
								try {
									flushPending();
								} catch (IOException e) {
									LOG.error(e.getMessage());
								}
							}
						}, 1);
						return;
					}
					if (priority || (sendBuffer.position() > MAX_PENDING_SIZE)) flushPending();
				}
				break;
			} catch(BufferOverflowException bofe) {
				flushPending();
				// Reallocating send buffer if bufferoverflow occurs too frequently
				if (sendBuffer.capacity() < MAX_BUFFER_SIZE) {
					if ((System.currentTimeMillis() - lastOverflow) < REALLOCATION_THRESHOLD)
						sendBuffer = ByteBuffer.allocateDirect(sendBuffer.capacity()*2);
					lastOverflow = System.currentTimeMillis();
				}
			}
		}		
	}
	
	private void flushPending() throws IOException {
		synchronized(sendBuffer) {
			if ((lastPos = sendBuffer.position()) > 0) {
				try {
					sendBuffer.flip();
					for(;;) {
						if (sendBuffer.position() >= sendBuffer.limit()) break;
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
	}
	
	private void sendPing(MsgHandler handler) throws IOException {
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
	public Integer requst(String subject, byte[] msg, Properties popts, MsgHandler handler) throws IOException {
		return request(subject, null, msg, popts, handler);
	}
	
	private Integer request(String subject, String msg, byte[] bmsg, Properties popts, MsgHandler handler) throws IOException {
		if (subject == null) return null;
    	
		String inbox = createInbox();
		Integer sub = subscribe(inbox, popts, handler);
		if (msg != null)
		publish(subject, inbox, msg, null);
		
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
					if (auto_unsubscribe) parent.unsubscribe(sid);
				} catch(IOException e) {
					LOG.error(e.getMessage());;
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
					sendBuffer.get(unsent, 0, lastPos);
				} catch(BufferUnderflowException e) {
					LOG.info(e.getMessage());
				}
				sendBuffer.clear();
				lastPos = 0;
			}

			conns.remove(this.toString());
			outer:
				for(; current < servers.length; current++) {
					for(; servers[current].reconnect_attempts < max_reconnect_attempts; servers[current].reconnect_attempts++) {
						try {
							channel.close();
							connect();

							if (isConnected()) {
								receiveBuffer.clear();
								status = AWAITING_CONTROL;
								sendConnectCommand();
								sendSubscriptions();
								if (unsent != null)
									sendCommand(unsent, unsent.length, false);
								flushPending();
								connStatus = OPEN;
								servers[current].reconnect_attempts = 0;
								break outer;
							}	
							Thread.sleep(reconnect_time_wait);
						} catch(IOException ie) {
							LOG.warn(ie.getMessage());
							continue;
						} catch (InterruptedException e) {
							LOG.warn(e.getMessage());
						}
					}
				}

			// Failing reconnection to all servers
			if (connStatus == RECONNECT) 
				throw new IOException("Failed connecting to all servers");
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
				LOG.error(e.getMessage());
			}
		}
	}
	
	/**
	 * Internal data structure to hold subscription meta data
	 * @author Teppei Yagihashi
	 */
	private class Subscription {
		public Integer sid = null;
		public String subject = null;
		public MsgHandler handler = null;
		public String queue = "";
		public int max = -1;
		public int received = 0;
		public int expected = -1;
		public TimerTask task = null;
    	
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
		private byte[] buf = new byte[INIT_BUFFER_SIZE];
		private int pos;
		private String subject;
		private String optReply;
		private int payload_length;
		private Subscription sub;
		
		private long lastTruncated;
		private boolean reallocate;

		public MsgProcessor() {
			for(int l = 0; l < INIT_BUFFER_SIZE; l++)
				buf[l] = '\0';		
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
				int diff = 0;
				receiveBuffer.flip();
				for(;;) {
					if (receiveBuffer.position() >= receiveBuffer.limit())
						break;
					switch(status) {
					case AWAITING_CONTROL :
						if ((pos = readNextOp(receiveBuffer)) == 0) {
							if (comp(buf, MSG, 3)) {
								status = AWAITING_MSG_PAYLOAD;
								parseMsg();
								if (receiveBuffer.limit() < (payload_length + receiveBuffer.position() + 2)) {
									diff = receiveBuffer.limit() - receiveBuffer.position();
									// Don't clear() yet and keep it in buffer
									receiveBuffer.compact();
									receiveBuffer.position(diff);
									reallocate = verifyTruncation();
									return;									
								}
								break;
							}
							else if (comp(buf, PONG, 4)) {
								MsgHandler handler;
								synchronized(pongs) {
									handler = pongs.poll();
								}
								processEvent(handler);
								if (handler.caller != null)
									handler.caller.interrupt();
							}
							else if (comp(buf, PING, 4)) sendCommand(PONG_RESPONSE, PONG_RESPONSE_LEN, true);
							else if (comp(buf, ERR, 4)) {
								if (connStatus == OPEN) {
									connStatus = RECONNECT;
									timer.schedule(new ReconnectTask(), 1);
								}
							}
							else if (comp(buf, OK, 3)) {/* do nothing for now */}
							else if (comp(buf, INFO, 4)) if (connectHandler != null) connectHandler.execute((Object)self);
						}
						break;
					case AWAITING_MSG_PAYLOAD :
						receiveBuffer.get(buf, 0, payload_length + 2);
						on_msg(); 
						pos = 0;
						status = AWAITING_CONTROL;					
						break;
					}
				}
				receiveBuffer.clear();
				
				// Reallocation occurs only when receiveBuffer is empty.
				if (reallocate) {
					receiveBuffer = ByteBuffer.allocateDirect(receiveBuffer.capacity()*2);
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
		
		private String getString() {
			return new String(buf, 0, payload_length);
		}
		
		private byte[] getByteArray() {
			byte[] arr = new byte[payload_length];
			System.arraycopy(buf, 0, arr, 0, payload_length);

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
		
		private int readNextOp(ByteBuffer buffer) {
			int i = pos, limit = buffer.limit();
			
			for(; buffer.position() < limit; i++) {
				buf[i] = buffer.get();
				if ((i > 0) && (buf[i] == '\n') && (buf[i-1] == '\r')) 
					return 0;
			}
			lastTruncated = System.currentTimeMillis();
			return i;
		}
		
		private boolean comp(byte[] src, byte[] dest, int length) {
			for(int i = 0; i < length; i++)
				if (src[i] != dest[i])
					return false;
			return true;
		}

		// Extracting MSG parameters
		private void parseMsg() {
			int index = 0, rid = 0, start = 0;
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