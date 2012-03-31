package nats;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Session {
	
	public static final String version = "0.2";
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

    public static final int MAX_BUFFER_SIZE = 327680;
    
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
    
    
    private static Selector selector;
    private static Session.SelectorThread selectorThread;
    private static int numConns;
    private static volatile int ssid;

    private Properties opts;
    private SocketChannel channel;
    private ByteBuffer receiveBuffer;
    private ByteBuffer sendBuffer;
    private int status;
    private ConcurrentLinkedQueue<byte[]> pendings;
    private ConcurrentHashMap<Integer, Subscription> subs;    
    private ConcurrentLinkedQueue<EventHandler> pongs;
    private Timer timer;
    
    private int msgs_sent;
    private int bytes_sent;
        
    static {
    	try {
    		ssid = 1;
    		numConns = 0;
    		selector = SelectorProvider.provider().openSelector();
    		selectorThread = new SelectorThread();
    	} 
    	catch (IOException e) {
    		e.printStackTrace();
    	}
    }
    
    private Session(Properties popts) throws IOException, InterruptedException {
    	receiveBuffer = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);
    	sendBuffer = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);
    	status = AWAITING_CONTROL;
    	msgs_sent = bytes_sent = 0;
    	subs = new ConcurrentHashMap<Integer, Subscription>();
    	pendings = new ConcurrentLinkedQueue<byte[]>();
    	pongs = new ConcurrentLinkedQueue<EventHandler>();
    	
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
    	if (!selectorThread.isAlive()) {
    		selectorThread.start();
    		while(!selectorThread.ready())
    			Thread.sleep(50);
    	}
    	this.sendCommand("CONNECT {\"verbose\":" + ((Boolean)opts.get("verbose")).toString() + ",\"pedantic\":" + ((Boolean)opts.get("pedantic")).toString() + "}" + CR_LF);
    	sendPing();
    	numConns++;
    }
    
    public void stop() throws IOException {
    	channel.close();
    	numConns--;
    	if (numConns == 0) {
    		selector.close();
    		try {
    			selectorThread.join();
    		}
    		catch(InterruptedException ie) {
    			ie.printStackTrace();
    		}
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
    	sendPing();
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
   		pendings.add(cmd.getBytes());
   		if (pendings.size() == 1) 
        	channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, this);
    }
    
    private void sendPing() throws IOException {
    	sendPing(null);
    }
    
    private void sendPing(EventHandler handler) throws IOException {
    	if (handler == null)
    		handler = emptyHandler;
    	pongs.add(handler);
    	sendCommand(PING_REQUEST);
    }
    
    private int write(byte[] b) throws IOException {
    	sendBuffer.clear();
		sendBuffer.put(b);
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
    		System.out.println("Pending messages are flushed.");
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
    
    private static class SelectorThread extends Thread {
		private volatile boolean running = false;
		
		public boolean ready() {
			return running;
		}
		
		public void run() {
			running = true;
			while(true) {
				try {
					int readyChannels = selector.select();
					if(readyChannels == 0) continue;
					Set<SelectionKey> keys = selector.selectedKeys();
					
					for(Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext();) {
						SelectionKey key = iter.next();
						SocketChannel ch = (SocketChannel)key.channel();
						Session session = (Session)key.attachment();
						if (key.isWritable()) {
				    		while ((session.pendings.size() > 0) && (session.write(session.pendings.peek()) > 0))
				    			session.pendings.poll();			// write succeeded and removed from pending list
				    		
				    		if (session.pendings.size() == 0)		// change back to listening mode
								key.interestOps(SelectionKey.OP_READ);
						}
						if (key.isReadable())
							processMessage(ch, session);
						if (key.isConnectable()) {
							System.out.println("finishconnect");
							ch.finishConnect();
						}
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
    
    private static void processMessage(SocketChannel ch, Session session) throws IOException {
    	if (ch.read(session.receiveBuffer) > 0) {
    		session.receiveBuffer.flip();
    		String[] msgs = read(session.receiveBuffer).split(CR_LF);
    		String[] params = null;
    		String op = null;
    		boolean fragmented = false;
    		
    		for(int i = 0; i < msgs.length; i++) {
    			op = msgs[i];
    			switch(session.status) {
    			case AWAITING_CONTROL :
    				if ((op.length() >= 3) && (op.substring(0,3).equals("MSG"))) {
    					params = op.split(SPC);
        				session.status = AWAITING_MSG_PAYLOAD;
    				}
        			else if (op.equals("PONG")) {
        				EventHandler handler = session.pongs.poll();
       					handler.execute(null);
       					if (handler.caller != null)
       						handler.caller.interrupt();
        			}
        			else if (op.equals("PING"))
        				session.sendCommand(PONG_RESPONSE);
        			else if ((op.equals("-ERR")) || (op.equals("+OK")) || (op.equals(""))) {
        				// do nothing for now
        			}
        			else if ((op.length() >= 4) && (op.substring(0,4).equals("INFO"))) {
        				// do nothing for now
        			}
       				else {
       					System.out.println("Unknown op : " + op + "@");
       					fragmented = true;
       				}
       				break;
    			case AWAITING_MSG_PAYLOAD :
    				Integer sid = Integer.valueOf(params[2]);
    				int length = Integer.parseInt(params[3]);
    				Subscription sub = (Subscription)session.subs.get(sid); 
    				sub.handler.execute(msgs[i]);
    				sub.received++;
    				session.status = AWAITING_CONTROL;
    				break;
    			}
    		}
			session.receiveBuffer.clear();
			if (fragmented) {
				// push back to the buffer
				session.receiveBuffer.put(op.getBytes());
				fragmented = false;
			}
    	}    	
    }

    private static byte[] buf = new byte[MAX_BUFFER_SIZE];
    private static String read(ByteBuffer buffer) {
    	String param = null;

    	buffer.get(buf, buffer.position(), buffer.limit()-buffer.position());
   		param = new String(buf, 0, buffer.position());
    	
   		return param;
    }

}
