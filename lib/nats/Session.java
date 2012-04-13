package nats;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Session {
	
	public static final String version = "0.2.1";
    public static final int DEFAULT_PORT = 4222;
    public static final String DEFAULT_PRE = "nats://localhost:";
    public static final String DEFAULT_URI = DEFAULT_PRE + Integer.toString(DEFAULT_PORT);

    public static final int MAX_CONTROL_LINE_SIZE = 512;

    // Parser state
    public static final int AWAITING_CONTROL = 0;
    public static final int AWAITING_MSG_PAYLOAD = 1;

    // Reconnect Parameters, 2 sec wait, 10 tries
    public static final int DEFAULT_RECONNECT_TIME_WAIT = 2*1000;
    public static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 10;

    public static final int MAX_BUFFER_SIZE = 32768;
    
    // Protocol
    public static final String CONTROL_LINE = "/^(.*)\r\n/";

    public static final String MSG = "/^MSG\\s+([^\\s\r\n]+)\\s+([^\\s\r\n]+)\\s+(([^\\s\r\n]+)[^\\S\r\n]+)?(\\d+)\r\n/i";
    public static final String OK = "/^\\+OK\\s*\r\n/i";
    public static final String ERR = "/^-ERR\\s+('.+')?\r\n/i";
    public static final String PING = "/^PING\r\n/i";
    public static final String PONG = "/^PONG\r\n/i";
    public static final String INFO = "/^INFO\\s+([^\r\n]+)\r\n/i";

    public static final String CR_LF = "\r\n";
    public static final int CR_LF_LEN = CR_LF.length();
    public static final String EMPTY = "";
    public static final String SPC = " ";

    // Protocol
    public static final String PUB = "PUB";
    public static final String SUB = "SUB";
    public static final String UNSUB = "UNSUB";
    public static final String CONNECT = "CONNECT";

    // Responses
    public static final String PING_REQUEST = "PING" + CR_LF;
    public static final String PONG_RESPONSE = "PONG" + CR_LF;

    // Pedantic Mode support
    public static final String Q_SUB = "/^([^\\.\\*>\\s]+|>$|\\*)(\\.([^\\.\\*>\\s]+|>$|\\*))*$/";
    public static final String Q_SUB_NO_WC = "/^([^\\.\\*>\\s]+)(\\.([^\\.\\*>\\s]+))*$/";

    public static final int FLUSH_THRESHOLD = 65536;
    
    private static int numSessions;
    private static volatile int ssid;
    private Selector selector;
    private Session.SelectorThread selectorThread;
    
    private Properties opts;
    private SocketChannel channel;
    private ByteBuffer receiveBuffer;
    private ByteBuffer sendBuffer;
    private int status;
    private byte[] pendings;
    private int pend_idx;
    private ConcurrentHashMap<Integer, Subscription> subs;    
    private LinkedList<EventHandler> pongs;
    private Timer timer;
    
    private int msgs_sent;
    private int bytes_sent;
        
    static {
   		ssid = 1;
   		numSessions = 0;
    }
    
    private Session(Properties popts) throws IOException, InterruptedException {
		selector = SelectorProvider.provider().openSelector();
		selectorThread = new SelectorThread();
    	receiveBuffer = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);
    	sendBuffer = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);
    	status = AWAITING_CONTROL;
    	msgs_sent = bytes_sent = 0;
    	subs = new ConcurrentHashMap<Integer, Subscription>();
    	pendings = new byte[MAX_BUFFER_SIZE];
    	pend_idx = 0;
    	pongs = new LinkedList<EventHandler>();
    	
    	opts = popts;
    	String[] uri = ((String)opts.get("uri")).split(":");
    	int port = Integer.parseInt(uri[2]);
    	
    	channel = SocketChannel.open();
    	channel.connect(new InetSocketAddress(uri[1].substring(2, uri[1].length()), port));
    	channel.configureBlocking(false);
    	channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ, this);    	

    	timer = new Timer("NATS_Timer");
    }
    
    private String hexRand(int limit, Random rand) {
    	return Integer.toHexString(rand.nextInt(limit));
    }
    
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

    public static Session connect(Properties popts) throws IOException, InterruptedException {
    	// Defaults
    	if (!popts.contains("verbose")) popts.put("verbose", new Boolean(false));
    	if (!popts.contains("pedantic")) popts.put("pedantic", new Boolean(false));
    	if (!popts.contains("reconnect")) popts.put("reconnect", new Boolean(false));
    	if (!popts.contains("ssl")) popts.put("ssl", new Boolean(false));
    	if (!popts.contains("max_reconnect_attempts")) popts.put("max_reconnect_attempts", new Integer(DEFAULT_MAX_RECONNECT_ATTEMPTS));
    	if (!popts.contains("reconnect_time_wait")) popts.put("reconnect_time_wait", new Integer(DEFAULT_RECONNECT_TIME_WAIT));
    	
    	// Overriding with ENV
    	if (System.getenv("NATS_URI") != null) popts.put("uri", System.getenv("NATS_URI")); else if (!popts.contains("uri")) popts.put("uri", DEFAULT_URI);
    	if (System.getenv("NATS_VERBOSE") != null) popts.put("verbose", new Boolean(System.getenv("NATS_VERBOSE")));
    	if (System.getenv("NATS_PEDANTIC") != null) popts.put("pedantic", new Boolean(System.getenv("NATS_PEDANTIC")));
    	if (System.getenv("NATS_DEBUG") != null) popts.put("debug", new Boolean(System.getenv("NATS_DEBUG")));
    	if (System.getenv("NATS_RECONNECT") != null) popts.put("reconnect", new Boolean(System.getenv("NATS_RECONNECT")));
    	if (System.getenv("NATS_FAST_PRODUCER") != null) popts.put("fast_producer", new Boolean(System.getenv("NATS_FAST_PRODUCER")));
    	if (System.getenv("NATS_SSL") != null) popts.put("ssl", new Boolean(System.getenv("NATS_SSL")));
    	if (System.getenv("NATS_MAX_RECONNECT_ATTEMPTS") != null) popts.put("max_reconnect_attempts", Integer.parseInt(System.getenv("NATS_MAX_RECONNECT_ATTEMPTS")));
    	if (System.getenv("NATS_MAX_RECONNECT_TIME_WAIT") != null) popts.put("max_reconnect_time_wait", Integer.parseInt(System.getenv("NATS_MAX_RECONNECT_TIME_WAIT")));

    	Session session = new Session(popts);

    	return session;
    }

    public void start() throws IOException, InterruptedException {
   		selectorThread.start();
   		while(!selectorThread.ready())
   			Thread.sleep(50);
   		
    	this.sendCommand("CONNECT {\"verbose\":" + ((Boolean)opts.get("verbose")).toString() + ",\"pedantic\":" + ((Boolean)opts.get("pedantic")).toString() + "}" + CR_LF);
    	numSessions++;
    }
    
    public void stop() throws IOException {
    	channel.close();
    	numSessions--;
    	if (numSessions == 0)
    		selector.close();
    	try {
    		selectorThread.running = false;
			selectorThread.join();
		}
		catch(InterruptedException ie) {
			ie.printStackTrace();
		}
    }
    
    public boolean isConnected() {
    	return channel.isConnected();
    }
    
    public void publish(String subject, String msg, String opt_reply, EventHandler handler) throws IOException {
    	if (subject == null) return;
    	if (msg != null) {
    		msgs_sent++;
    		bytes_sent += msg.length();
    	}
    	sendCommand("PUB " + subject + " " + ((opt_reply == null) ? "" : opt_reply) + " " + Integer.toString(msg.length()) + CR_LF + msg + CR_LF);
    }
    
    public Integer subscribe(String subject, EventHandler handler) throws IOException {
    	return this.subscribe(subject, null, handler);
    }
    
    public Integer subscribe(String subject, Properties popts, EventHandler handler) throws IOException {
    	Integer sid = ssid++;
    	subs.put(sid, new Subscription(subject, handler));
    	
    	String queue = " ";
    	Integer max = null;
    	if (popts != null) {
    		queue = (popts.getProperty("queue") == null ? " " : (String)popts.getProperty("queue"));
        	max = (Integer)popts.get("max");
    	}
    	sendCommand("SUB " + subject + " " + queue + sid.toString() + CR_LF);
    	
    	if (max != null) this.unsubscribe(sid, max.intValue());
    	
    	return sid;
    }
    
    public void unsubscribe(Integer sid) throws IOException {
    	this.unsubscribe(sid, 0);
    }

    public void unsubscribe(Integer sid, int opt_max) throws IOException {
    	Subscription sub = subs.get(sid);
    	if (sub == null) return;
    	if (opt_max < 0) opt_max = 0;
    	
    	sendCommand("UNSUB " + sid.toString() + " " + Integer.toString(opt_max) + CR_LF);
    	
    	if (sub.received >= opt_max) subs.remove(sid);
    }
    
    public int getSubscriptionCount() {
    	return subs.size();
    }
    
    public void timeout(final Integer sid, long tout, Properties prop, final EventHandler handler) {
    	Subscription sub = subs.get(sid);
    	if (sub == null) return;
    	final boolean auto_unsubscribe = ((Boolean)prop.get("auto_unsubscribe")).booleanValue();
    	if (sub.task != null) sub.task.cancel();

    	final Session parent = this;
    	TimerTask task = new TimerTask() {
    		public void run() {
    			try {
    				if (auto_unsubscribe) parent.unsubscribe(sid);
    			}
    			catch(IOException e) {
    				e.printStackTrace();
    			}
    			if (handler != null) handler.execute(sid);
    		}
    	}; 
    	timer.schedule(task, tout * 1000);
    }
    
    private void sendCommand(String cmd) throws IOException {
   		int length = cmd.length();
   		
   		if ((channel.keyFor(selector).interestOps() & SelectionKey.OP_WRITE) == 0)
        	channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, this);

    	if (pend_idx >= MAX_BUFFER_SIZE - length - 6)
   			flush();

    	append(cmd, length);
    }
    
    private void append(String str, int length) {
   		synchronized(pendings) { 
    		System.arraycopy(str.getBytes(), 0, pendings, pend_idx, length);
    		pendings[pend_idx + length] = '\0';
    		pend_idx += length;
   			pendings.notify();
    	}    	    	
    }
    
    private void sendPing() throws IOException {
    	sendPing(null);
    }
    
    private void sendPing(EventHandler handler) throws IOException {
    	if (handler == null)
    		handler = emptyHandler;
    	
    	synchronized(pongs) {
    		pongs.add(handler);
    		pongs.notify();
    	}
    	append(PING_REQUEST, 6);
    }
    
    private int write(byte[] b) throws IOException {
    	sendBuffer.clear();
		sendBuffer.put(b, 0, pend_idx);
		sendBuffer.flip();
    	
    	return channel.write(sendBuffer);
    }
    
    public void flush() {
    	flush(emptyHandler);
    }

    public void flush(EventHandler handler) {
    	try {
    		handler.caller = Thread.currentThread();
    		sendPing(handler);
   			handler.caller.join();
    	}
    	catch(Exception e) {
    		// System.out.println("Pending messages are flushed.");
    	}
    }
    
    public String inspect() {
    	return "<nats java " + version + ">";
    }
    
    
    // Event processing and main loop
    public abstract class EventHandler {
    	public Thread caller;
    	public void execute(Object o) {}
    }

    private EventHandler emptyHandler = new EventHandler() {};
    
    public class Subscription {
    	public String subject = null;
    	public EventHandler handler = null;
    	public int received = 0;
    	public TimerTask task = null;
    	
    	public Subscription(String psubject, EventHandler phandler) {
    		subject = psubject;
    		handler = phandler;
    	}
    }
    
    private class SelectorThread extends Thread {
		private volatile boolean running = false;
		
		public boolean ready() {
			return running;
		}
		
		public void run() {
			running = true;
			while(running) {
				try {
					int readyChannels = selector.select(5);
					if(readyChannels == 0) continue;
					Set<SelectionKey> keys = selector.selectedKeys();
					
					for(Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext();) {
						SelectionKey key = iter.next();
						Session session = (Session)key.attachment();
						if (key.isWritable()) {
							if (session.pend_idx > 0) {
								synchronized(session.pendings) {
									session.pend_idx -= session.write(session.pendings);
									session.pendings.notify();
								}
							} 
							// change back to listening mode														
							if (session.pend_idx == 0) 
								key.interestOps(SelectionKey.OP_READ);
						}
						else if (key.isReadable())
							session.processMessage();
						iter.remove();
					}
				}
				catch(CancelledKeyException cke) {
					cke.printStackTrace();
					continue;
				}
				catch(ClosedSelectorException cse) {
					cse.printStackTrace();
					break;
				}
				catch(Exception e) {
					e.printStackTrace();
					break;
				}	
			}
			System.out.println("SelectorThread exiting");
		}    	
    }
    
	private String[] params = null;
    private void processMessage() throws IOException {
    	if (channel.read(receiveBuffer) > 0) {
    		receiveBuffer.flip();
    		String[] msgs = read(receiveBuffer).split(CR_LF);
    		String op = null;
    		boolean fragmented = false;
    		
    		for(int i = 0; i < msgs.length; i++) {
    			op = msgs[i];
    			switch(status) {
    			case AWAITING_CONTROL :
        			if ((op.length() > 1) && (op.charAt(0) == '\n')) 
        				op = op.substring(1, op.length()); // eliminating LF at front
        			if ((op.length() >= 3) && (op.substring(0,3).equals("MSG"))) {
    					params = op.split(SPC);
    					if (params.length >= 4)
    						status = AWAITING_MSG_PAYLOAD;
    					else 
    						fragmented = true;
    				}
    				else if (op.equals("PONG")) {
    					EventHandler handler = null;
    					synchronized(pongs) {
    						handler = pongs.poll();
    					}
    					handler.execute(null);
    					if (handler.caller != null)
    						handler.caller.interrupt();
    				}
    				else if (op.equals("PING"))	sendCommand(PONG_RESPONSE);
    				else if ((op.equals("-ERR")) || (op.equals("+OK")) || (op.equals(""))) {/* do nothing for now */}
    				else if ((op.length() >= 4) && (op.substring(0,4).equals("INFO"))) {/* do nothing for now */}
    				else fragmented = true; /* Unknown operation, possibly fragmented */
    				break;
    			case AWAITING_MSG_PAYLOAD :
    				Integer sid = Integer.valueOf(params[2]);
    				int length = Integer.parseInt(params[3].trim());
    				Subscription sub = (Subscription)subs.get(sid);
    				if (length > op.length())
    					fragmented = true;
    				else {
    					sub.handler.execute(op);
    					sub.received++;
    					status = AWAITING_CONTROL;
    				}
    				break;
    			}
    			receiveBuffer.clear();
    			// The command is not fully read yet. Push back to the buffer.
    			if (fragmented) {
   					receiveBuffer.put(op.getBytes());
    				fragmented = false;
    			}
    		}
    	}    	
    }

    private byte[] buf = new byte[MAX_BUFFER_SIZE];
    private String read(ByteBuffer buffer) {
    	String param = null;
    	buffer.get(buf, buffer.position(), buffer.limit()-buffer.position());
   		param = new String(buf, 0, buffer.position());
   		return param;
    }

}
