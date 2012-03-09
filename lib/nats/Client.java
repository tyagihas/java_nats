package nats;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class Client {
	
	public static final String version = "0.1";
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
    
    
    private static Selector selector;
    private static Client.SelectorThread selectorThread;

    private int status;
    private Properties opts;
    private SocketChannel channel;
    private SelectionKey skey;
    private ByteBuffer sendBuffer;
    private ByteBuffer receiveBuffer;
    
    private int msgs_sent;
    private int bytes_sent;
    
    private static volatile int ssid;
    private ConcurrentHashMap<Integer, Subscription> subs;
    
    private Timer timer;
    
    static {
    	try {
    		ssid = 1;
    		selector = SelectorProvider.provider().openSelector();
    		selectorThread = new SelectorThread();
    	} 
    	catch (IOException e) {
    		e.printStackTrace();
    	}
    }
    
    private Client(Properties popts) throws IOException, InterruptedException {
    	sendBuffer = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);
    	receiveBuffer = ByteBuffer.allocateDirect(MAX_BUFFER_SIZE);
    	msgs_sent = bytes_sent = 0;
    	status = AWAITING_CONTROL;
    	subs = new ConcurrentHashMap<Integer, Subscription>();
    	
    	opts = popts;
    	String[] uri = ((String)opts.get("uri")).split(":");
    	int port = Integer.parseInt(uri[2]);
    	
    	channel = SocketChannel.open();
    	channel.connect(new InetSocketAddress(uri[1].substring(2, uri[1].length()), port));
    	channel.configureBlocking(false);
    	skey = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ, this);    	
    	
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

    public static Client connect(Properties popts) throws IOException, InterruptedException {
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

    	Client client = new Client(popts);

    	return client;
    }

    public void start() throws IOException, InterruptedException {
    	if (!selectorThread.isAlive()) {
    		selectorThread.start();
    		while(!selectorThread.ready())
    			Thread.sleep(100);
    	}
    	this.sendCommand("CONNECT {\"verbose\":" + ((Boolean)opts.get("verbose")).toString() + ",\"pedantic\":" + ((Boolean)opts.get("pedantic")).toString() + "}\"");
    }
    
    public void stop() throws IOException {
    	channel.close();
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
    	
    	this.sendCommand("PUB " + subject + " " + ((opt_reply == null) ? "" : opt_reply) + " " + Integer.toString(msg.length()) + CR_LF + msg);
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
    	sendCommand("SUB " + subject + " " + queue + sid.toString());
    	
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

    	final Client parent = this;
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
    	cmd = cmd + CR_LF + PING_REQUEST;
    	sendBuffer.clear();
    	sendBuffer.put(cmd.getBytes());
    	sendBuffer.flip();
    	int num = 0;
    	while(sendBuffer.hasRemaining())
    	    num += channel.write(sendBuffer);
    }
    
    public String inspect() {
    	return "<nats client " + version + ">";
    }
    
    
    // Event processing and main loop
    public interface EventHandler {
    	public void execute(Object o);
    }
    
    public class Subscription {
    	public String subject = null;
    	public EventHandler handler = null;
    	public int received = 0;
    	public TimerTask task;
    	
    	public Subscription(String psubject, EventHandler phandler) {
    		subject = psubject;
    		handler = phandler;
    		received = 0;
    		task = null;
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
						SocketChannel ch = (SocketChannel) key.channel();
						if (key.isConnectable()) {
							ch.finishConnect();
							continue;
						}
						if (key.isReadable()) {
							Client client = (Client)key.attachment();
							int bytesRead = ch.read(client.receiveBuffer);
							client.receiveBuffer.flip();

							if (bytesRead > 0) {
								String[] params = client.parse(client.receiveBuffer);
								if (params[0].equals("MSG")) {
									Subscription sub = (Subscription)client.subs.get(Integer.parseInt(params[2])); 
									sub.handler.execute(params[4]);
									sub.received++;
								}
								client.receiveBuffer.clear();
							}
						}
						iter.remove();
					}
				}
				catch(Exception ie) {
					ie.printStackTrace();
				}	
			}
		}    	
    }
    
    private String[] parse(ByteBuffer buffer) {
    	String[] params = new String[6];

    	params[0] = read(buffer, 3); // Reading op code
		buffer.compact();
		buffer.flip();
    	if (params[0].equals("MSG")) {
    		StringTokenizer st = new StringTokenizer(read(buffer, buffer.limit()));
    		int i = 1;
    		for(; st.hasMoreTokens(); i++) {
    			params[i] = st.nextToken();
    			if (i == 4) {
    				StringBuffer sb = new StringBuffer(params[i]);
    				while(st.hasMoreTokens())
    					sb.append(" ").append(st.nextToken());
    				params[i] = sb.toString();
    				break;
    			}
    		}
    	}
   		//for(int i = 0; params[i] != null; i++)
		//	System.out.println("param[" + Integer.toString(i) + "] : "+ params[i]);

    	return params;
    }
    
    private static String read(ByteBuffer buffer, int count) {
    	String param = null;
    	if (count <= buffer.limit()) {    	
    		byte[] b = new byte[count];
    		buffer.get(b);
    		param = new String(b);
    	}    	
    	return param;
    }

}
