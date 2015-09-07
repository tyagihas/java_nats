package org.nats;

class Server {
	private final String host;
	private final int port;
	private final String user;
	private final String pass;
	
	private boolean connected = false;
	private int reconnect_attempts = 0;

	public Server(String host, int port) {
		this(host,port,null,null);
	}
	public Server(String host, int port, String user, String pass) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.pass = pass;
	}
	
	public void markFailedToConnect(){
		connected = false;
		++reconnect_attempts;
	}
	
	public void markConnectedSuccessfully(){
		connected = true;
		reconnect_attempts=0;
	}
	
	public boolean isConnected(){
		return connected;
	}
	
	public boolean mayTryToReconnect(int reconnectsLimit){
		return reconnect_attempts<reconnectsLimit;
	}
	public String getHost() {
		return host;
	}
	public int getPort() {
		return port;
	}
	public String getUser() {
		return user;
	}
	public String getPass() {
		return pass;
	}
	
	@Override
	public String toString() {
		return "Server [host=" + host + ", port=" + port + ", user=" + user + ", pass=" + pass + ", connected=" + connected + ", reconnect_attempts=" + reconnect_attempts + "]";
	}
}
