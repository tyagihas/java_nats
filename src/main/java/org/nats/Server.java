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

package org.nats;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.nats.common.Constants.TLS_REQUIRED;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server holds a list of Nats server and attributes returned from "INFO" operation. "next" method 
 * rotates server list for failover.
 * @author Teppei Yagihashi
 */
public class Server {
  private Logger LOG = Logger.getLogger(Server.class.getName());
  private static ConcurrentLinkedQueue<Server> servers;

  static {
    servers = new ConcurrentLinkedQueue<Server>();
  }
	
  /**
   * Parse opts properties and build Nats server list.
   * @param opts Properties object containing server uris
  */
  public static void addServers(Properties opts) {
    String[] serverStrings = null;
    if (opts.containsKey("uris")) {
      serverStrings = ((String)opts.get("uris")).split(",");
    }
    else if (opts.containsKey("servers")) {
      serverStrings = ((String)opts.get("servers")).split(",");
    }
    else if (opts.containsKey("uri")) {
      serverStrings = ((String)opts.get("uri")).split(",");
    }

    Random rand = (((Boolean)opts.get("dont_randomize_servers"))
        == Boolean.TRUE ? null : new Random());
    String[] uri;
    for(int i = 0; i < serverStrings.length; i++) {
      int idx = i;
      for(; rand != null; ) {
        idx = rand.nextInt(serverStrings.length);
        if (serverStrings[idx] != null) { break; }
      }
      uri = serverStrings[idx].split(":");
      Server server = new Server();
      if (serverStrings[idx].contains("@")) {
        server.user = uri[1].substring(2, uri[1].length());
        server.pass = uri[2].split("@")[0];
        server.host = uri[2].split("@")[1];
        server.port = Integer.parseInt(uri[3]);
      }
      else {
        server.user = null;
        server.pass = null;
        server.host = uri[1].substring(2, uri[1].length());
        server.port = Integer.parseInt(uri[2]);
      }
      serverStrings[idx] = null;
      servers.add(server);
    }
  }

  /**
   * Rotate the server list and return the current server.
   */
  public static Server next() {
    // Rotating the server list
    servers.add(servers.poll());
    return servers.peek();
  }
	
  /**
   * Return the current server.
   */
  public static Server current() {
    return servers.peek();
  }
	
  private String user;
  private String pass;
  private String host;
  private int port;
  private boolean connected;
  private int maxPayload;
  private HashMap<String, String> info;
	
  private Server() { info = new HashMap<String, String>(); }
		
  public String getUser() { return user; }
  public String getPass() { return pass; }
  public String getHost() { return host; }
  public int getPort() { return port; }
  public void setConnected(boolean connected) { this.connected = connected; }
  public boolean getConnected() { return connected; }
  public void setMaxPayload(int maxPayload) { this.maxPayload = maxPayload; }
  public int getMaxPayload() { return maxPayload; }

  /**
   * Parse id, version, lang, go, auth, ssl attributes.
   */
  public void parseServerInfo(String serverInfo) {
    String[] attrs = serverInfo.substring(6, serverInfo.length()-2).split(",");
    for(int i = 0; i < attrs.length - 1; i++) {
      String[] kv = attrs[i].split(":");
      String key = kv[0].substring(1, kv[0].length()-1);
	  int trim = 0;
      if (kv[1].startsWith("\"")) { trim = 1; }
        String value = kv[1].substring(trim, kv[1].length() - trim);
        info.put(key, value);
        LOG.log(Level.INFO, "info : " + key + "=" + value);
      }
  }
	
  /**
   * Retrieve a specific server attribute.
   * @param key String object corresponding to a server attribute
   */
  public String getServerInfo(String key) { return info.get(key); }

  /**
   * Check whether TLS connection is required.
   * @return boolean value to indicate whether TLS is required
   */
  public boolean isTLS() {
    return (info.containsKey(TLS_REQUIRED) ? Boolean.parseBoolean(info.get(TLS_REQUIRED)) : false);
  }
}
