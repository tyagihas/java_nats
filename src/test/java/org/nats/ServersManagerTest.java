package org.nats;

import org.junit.Test;

import org.junit.Assert;

public class ServersManagerTest {

	@Test(expected = IllegalArgumentException.class)
	public void noServers() {
		ServersManager manager = new ServersManager(new Server[0], 5);
		manager.getServer();
	}

	@Test(expected = IllegalArgumentException.class)
	public void nullServers() {
		ServersManager manager = new ServersManager(null, 5);
		manager.getServer();
	}

	@Test
	public void singleServer() {
		Server server = new Server("localhost",4242);
		ServersManager manager = new ServersManager(new Server[] { server }, 5);
		Assert.assertEquals(server, manager.getServer());
	}

	@Test
	public void connectFailuresAndThenSuccess() {
		Server[] servers = new Server[] { new Server("localhost",4242), new Server("localhost",4242), new Server("localhost",4242) };
		ServersManager manager = new ServersManager(servers, 5);
		for (int i = 0; i < 15; ++i) {
			int pos = i / 5;
			Assert.assertEquals(manager.getNextToConnectTo(), servers[pos]);
			servers[pos].markFailedToConnect();
		}

		Assert.assertEquals(manager.getNextToConnectTo(), null);

		servers[1].markConnectedSuccessfully();

		Assert.assertEquals(manager.getNextToConnectTo(), servers[1]);

		servers[0].markConnectedSuccessfully();
		
		for (int i=0;i<5;i++){
			Assert.assertEquals(manager.getNextToConnectTo(), servers[1]);
			servers[1].markFailedToConnect();
		}

		Assert.assertEquals(manager.getNextToConnectTo(), servers[0]);
	}
}
