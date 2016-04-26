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
FROM, OUT OF OR IN resourceECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
**/

package org.nats.common;

import static org.nats.common.Constants.DEFAULT_PING_INTERVAL;

import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

import org.nats.MsgHandler;
import org.nats.server.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NatsMonitor monitors a registered Resource by sending a PING message. It removes the resource
 * from the list when PIGN fails. 
 * @author Teppei Yagihashi
 */
public class NatsMonitor extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(NatsMonitor.class);
	
	/**
	 * Target resources need to implement Resource interface to be monitored by NatsMonitor.
	 */
	public interface Resource {
		public void sendPing(MsgHandler handler) throws IOException;
		public String getResourceId();
		public boolean isConnected();
	}
	
	private ConcurrentHashMap<String, Resource> resources;
	private static NatsMonitor monitor = null;
	
	/**
	 * Return a NatsMonitor singleton instance.
	 * @return Returning a NatsMonitor instance
	 */	
	public static synchronized NatsMonitor getInstance() {
		if (monitor == null) { monitor = new NatsMonitor(); }
		return monitor;
	}
	
	private NatsMonitor() {
		super("NatsMonitor");
		setDaemon(true);	
		resources = new ConcurrentHashMap<String, Resource>();
	}
	
	/** 
	 * Add targeting Resource to the list. 
	 * @param id String object uniquely identifying the resource
	 * @param resource Resource object to be monitored by NatsMonitor
	 */
	public void addResource(String id, Resource resource) {
		resources.put(id, resource);
	}
	
	/** 
	 * Remove a resource from the list. 
	 * @param id String object uniquely identifying the resource
	 */
	public void removeResource(String id) {
		resources.remove(id);
	}

	@Override
	public void run() {
		LOG.debug("Starting NatsMonitor");
		MsgHandler handler = new MsgHandler() { };
		int ping_interval = (System.getenv("NATS_PING_INTERVAL") == null) 
				? DEFAULT_PING_INTERVAL : Integer.parseInt(System.getenv("NATS_PING_INTERVAL"));
		Resource resource = null;
		while (true) {
			try {
				Thread.sleep(ping_interval);
				for(Enumeration<Resource> e = resources.elements(); e.hasMoreElements();) {
					resource = e.nextElement();
					if ((resource != null) && (resource.isConnected())) { resource.sendPing(handler); }
				}
			} catch(IOException ioe) {
				LOG.debug(ioe.getMessage() + ", " + "Failed pinging resoure(" + 
						resource.getResourceId() + ")");
				Subscription.removeSubscribers(resource.getResourceId());
				this.removeResource(resource.getResourceId());
			} catch(InterruptedException ie) {
				LOG.info(ie.getMessage());
				break;
			}
		}
		LOG.debug("Stopping NatsMonitor");
	}
}
