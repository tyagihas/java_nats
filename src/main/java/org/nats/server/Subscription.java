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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.nats.common.NatsMonitor;
import org.nats.common.Tokenizer;

import static org.nats.common.Constants.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Subscription {	
	private static Logger LOG = Logger.getLogger(Subscription.class.getName());
	private static Tokenizer tokenizer;

	private static ConcurrentHashMap<String, Subscription> subList;
	private static ConcurrentHashMap<String, Subscription> subNodes;
	
	static {
		tokenizer = new Tokenizer();
		subList = new ConcurrentHashMap<String, Subscription>();
		subNodes = new ConcurrentHashMap<String, Subscription>();
	}
	
	public static void register(ClientConnection conn, String tokenString, String queueGroup, String sid) {
		tokenizer.setToken(tokenString);
		tokenizer.next();
		String token = tokenizer.getToken();
		Subscription sub = subNodes.get(token);
		if (sub == null) {
			sub = new Subscription();
			subNodes.put(token, sub);
		}
		register(sub, conn, token, queueGroup, sid);
	}
	
	private static void register(Subscription sub, ClientConnection conn, String token, String queueGroup, String sid) {
		if (!tokenizer.isNext()) {
			String id = conn.getResourceId() + "_" + sid;
			Subscriber subscriber = null;
			if (queueGroup == null) { 
				subscriber = new Subscriber(id, queueGroup, conn);				
				sub.subscribers.put(id, subscriber);
			}	
			else {
				subscriber = sub.subscribers.get(queueGroup);
				if (subscriber == null) { 
					subscriber = new Subscriber(null, queueGroup, null);
					sub.subscribers.put(queueGroup, subscriber);
				}
				subscriber.queue(new Subscriber(id, queueGroup, conn));
			}
			subList.put(id, sub);
		}
		else {
			tokenizer.next();
			String nextToken = tokenizer.getToken();
			Subscription child = sub.children.get(nextToken);
			if (child == null) { 
				child = new Subscription();
				sub.children.put(nextToken, child);
			}
			register(child, conn, nextToken, queueGroup, sid);
		}
	}
	
	public static void unregister(String clientId, String sid, int maxMessages) {
		String id = clientId + "_" + sid;
		Subscription sub = subList.get(id);
		Subscriber subscriber = sub.subscribers.get(id);
		if (subscriber != null) {
			if (maxMessages == -1) { sub.subscribers.remove(id); }
			else { subscriber.setMaxMessages(maxMessages); }
		}
		else {
			for (Iterator<Entry<String, Subscriber>> iter = sub.subscribers.entrySet().iterator(); iter.hasNext();) {
				subscriber = iter.next().getValue();
				if (subscriber.isQueueGroup()) {
					for (Iterator<Subscriber> qiter = subscriber.queue.iterator(); qiter.hasNext();) {
						Subscriber qsubscriber = qiter.next();
						if (qsubscriber.id.equals(id)) {
							if (maxMessages == -1) { subscriber.queue.remove(qsubscriber); }
							else { qsubscriber.setMaxMessages(maxMessages);}
							break;
						}
					}
				}
			}
		}
	}
	
	public static void removeSubscribers(String clientId) {
		NatsMonitor.getInstance().removeResource(clientId);
		for (Iterator<Subscription> iter = subList.values().iterator(); iter.hasNext();) {
			Subscription sub = iter.next();
			for (Iterator<Subscriber> siter = sub.subscribers.values().iterator(); siter.hasNext(); ) {
				Subscriber subscriber = siter.next();
				if (subscriber.isQueueGroup()) {
					for (Iterator<Subscriber> qiter = subscriber.queue.iterator(); qiter.hasNext(); ) {
						Subscriber qsubscriber = qiter.next();
						if (qsubscriber.conn.getResourceId().equals(clientId)) {
							subList.remove(subscriber.id);
							subscriber.queue.remove(qsubscriber);
						}
					}
				}
				else {
					if (subscriber.conn.getResourceId().equals(clientId)) {
						subList.remove(subscriber.id);
						sub.subscribers.remove(subscriber.id);
					}
				}
			}
		}
	}

	public static void message(ClientConnection src, String tokens) {
		tokenizer.setToken(tokens);
		tokenizer.next();
		Subscription sub = subNodes.get(tokenizer.getToken());
		if (sub == null) { return; }
		message(src, sub, false);
	}
	
	private static void message(ClientConnection src, Subscription sub, boolean all) {
		if ((!tokenizer.isNext()) || (all)){
			for (Iterator<Entry<String, Subscriber>> iter = sub.subscribers.entrySet().iterator(); iter.hasNext(); ) {
				Subscriber subscriber = iter.next().getValue();
				if (subscriber.isQueueGroup()) {
					Subscriber qsubscriber = null;
					while (subscriber.hasNext()) {
						qsubscriber = subscriber.next();
						if (sendMessage(qsubscriber, src)) {
							if (qsubscriber.incr()) { subscriber.queue.remove(); }
							break;
						}
						else { subscriber.queue.remove(); }
					}
				}
				else { 
					if (sendMessage(subscriber, src)) { if (subscriber.incr()) { sub.subscribers.remove(subscriber.id); } } 
					else { sub.subscribers.remove(subscriber.id); }
				}				
				
				if ((all) && (sub.children.size() > 0)) { processNext(src, sub, all); }
			}
		}
		else { processNext(src, sub, all); }
	}
	
	private static boolean sendMessage(Subscriber subscriber, ClientConnection src) {
		try {
			src.msg(subscriber.id.split("_")[1], subscriber.conn);
			return true;
		} catch (IOException e) {
			subList.remove(subscriber.id);
			NatsMonitor.getInstance().removeResource(subscriber.conn.getResourceId());
			LOG.log(Level.SEVERE, e.getClass().getName() + ", " + "Failed publishing to Client(" + subscriber.conn.getResourceId() + ")");
		}
		return false;
	}
	
	private static void processNext(ClientConnection src, Subscription sub, boolean all) {
		boolean isAll = all;
		tokenizer.next();
		for (Iterator<Entry<String, Subscription>> iter = sub.children.entrySet().iterator(); iter.hasNext();) {
			Entry<String, Subscription> entry = iter.next();
			if ((tokenizer.compare(entry.getKey()) || (entry.getKey().equals(WC)) || isAll || (isAll = entry.getKey().equals(ARR)))) { 
				message(src, entry.getValue(), isAll);
				break;
			}
		}		
	}
	
	private static class Subscriber {
		private String id;
		private String queueGroup;
		private ClientConnection conn;
		private long maxMessages;
		private long messageCount;
		private ConcurrentLinkedQueue<Subscriber> queue;
		
		public Subscriber(String id, String queueGroup, ClientConnection conn) {
			this.id = id;
			this.queueGroup = queueGroup;
			if (isQueueGroup()) { queue = new ConcurrentLinkedQueue<Subscriber>(); }
			this.conn = conn;
			maxMessages = -1;
			messageCount = 0;
		}
				
		public void setMaxMessages(long maxMessages) { this.maxMessages = maxMessages; }
		public boolean isExpired() { return (maxMessages != -1) && (maxMessages == messageCount); }
		public boolean isQueueGroup() { return queueGroup != null; }
		public boolean incr() { 
			messageCount++;
			return isExpired();
		}
		public void queue(Subscriber subscriber) { queue.add(subscriber); }
		public Subscriber next() {
			queue.add(queue.poll());
			return queue.peek();
		}
		public boolean hasNext() { return queue.size() > 0; }
	}
	
	private ConcurrentHashMap<String, Subscription> children;
	private ConcurrentHashMap<String, Subscriber> subscribers;
	
	public Subscription() {
		children = new ConcurrentHashMap<String, Subscription>();
		subscribers = new ConcurrentHashMap<String, Subscriber>();
	}	
}

