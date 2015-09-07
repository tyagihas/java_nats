package org.nats;

class ServersManager{
	private final Server[] servers;
	private int maxReconnectAttempts;
	private int current = 0;

	public ServersManager(Server[] servers, int maxReconnectAttempts){
		if (servers==null || servers.length==0){
			throw new IllegalArgumentException("no servers passed to constructor");
		}
		this.maxReconnectAttempts=maxReconnectAttempts;
		this.servers=servers;
	}

	public Server getServer(){
		return servers[current];
	}
	
	public Server getNextToConnectTo(){
		int serversWithNoMoreReconnects = 0;
		
		//loop over servers until we find one that has reconnects left
		while(serversWithNoMoreReconnects<servers.length &&
				  !servers[current].mayTryToReconnect(maxReconnectAttempts)){
					++serversWithNoMoreReconnects;
			current = (current + 1) % servers.length;
		}
		
		if (serversWithNoMoreReconnects>=servers.length){
			return null;
		} else {
			return getServer();
		}
	}
}
