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

import static org.nats.common.Constants.*;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.nats.common.NatsMonitor;
import org.nats.common.NatsUtil;
import org.nats.MsgHandler;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientConnection extends Thread implements NatsMonitor.Resource {
	private Logger LOG = Logger.getLogger(ClientConnection.class.getName());

	private String clientId;
	private SocketChannel channel;
	private ByteBuffer sendBuffer;
	private ByteBuffer receiveBuffer;
	private int pos;

	private NatsUtil nUtil;
	
	private String param1;
	private String param2;
	private String param3;
	private int length;
	private String body;
	
	private byte[] cmd;

	public ClientConnection(String clientId, SocketChannel channel) {
		this.setDaemon(true);
		this.clientId = clientId;
		this.channel = channel;
		sendBuffer = ByteBuffer.allocate(INIT_BUFFER_SIZE);
		receiveBuffer = ByteBuffer.allocate(INIT_BUFFER_SIZE);
		cmd = new byte[INIT_BUFFER_SIZE];
		nUtil = new NatsUtil();
		pos = 0;
		length = -1;
		LOG.log(Level.ALL, this.toString() + " Client(" + clientId + ") initialized");
	}
	
	@Override
	public String getResourceId() {
		return clientId;
	}
	
	@Override
	public boolean isConnected() {
		return channel.isConnected();
	}

	@Override
	public void run() {
		try {
			while (!channel.isConnected()) {
				Thread.sleep(200);
			}

			while (true) {
				try {
					read();
				} catch (IOException e) {
					LOG.info(e.getMessage() + ", " + this.toString() + " Client(" + clientId + ")");
					if (channel != null) { channel.close(); }
					break;
				}
			}
		} catch (IOException | InterruptedException e) {
			LOG.info(e.getMessage() + ", " + this.toString() + " Client(" + clientId + ")");
		}
		Subscription.removeSubscribers(clientId);
	}

	private void read() throws IOException {		
		if (channel.read(receiveBuffer) > 0) {
			receiveBuffer.flip();
			while (true) {
				if (receiveBuffer.position() >= receiveBuffer.limit()) { break; }
				// Processing unread payload
				if (length != -1) { payload(); }
				
				if ((pos = nUtil.readNextOp(pos, receiveBuffer)) == 0) {
					if (nUtil.compare(PUB, 3)) {
						parse();
						if (receiveBuffer.limit() > length + receiveBuffer.position() + 2) { payload(); }
						else {
							receiveBuffer.compact();
							return;
						}
					}
					else if (nUtil.compare(PING, 4)) { pong(); }
					else if (nUtil.compare(PONG, 4)) { /* do nothing */ }
					else if (nUtil.compare(SUB, 3)) { 
						parse(); 
						Subscription.register(this, param1, param2, param3);
						param1 = param2 = param3 = null;
						length = -1;
					}
					else if (nUtil.compare(UNSUB, 5)) {
						String[] op = nUtil.getOp().split(" ");
						int maxMessages = -1;
						if (op.length == 3) { maxMessages = Integer.parseInt(op[2]); }
						Subscription.unregister(clientId, op[1], maxMessages);
					}
					else if (nUtil.compare(CONNECT, 7)) { info(); }
				}
			}
			receiveBuffer.clear();
		}
	}
	
	private synchronized void sendCommand(byte[] data, int len) throws IOException {
		sendBuffer.put(data, 0, len);
		sendBuffer.flip();
		while (true) {
			if (sendBuffer.position() >= sendBuffer.limit()) { break; }
			channel.write(sendBuffer);
		}
		sendBuffer.clear();
	}
	
	private void pong() throws IOException {
		sendCommand(PONG_RESPONSE, PONG_RESPONSE_LEN);
	}
	
	private void info() throws IOException {
		StringBuffer info = new StringBuffer(new String(INFO));
		
		info.append(" {\"server_id\":\"NatsEmbeddedServer\",");
		info.append("\"version\":\"" + version + "\",");
		info.append("\"go\":\"0\",");
		info.append("\"host\":\"" + InetAddress.getLocalHost().getHostAddress() + "\",");
		info.append("\"port\":" + NatsEmbeddedServer.get("port") + ",");
		info.append("\"auth_required\":" + (Boolean)NatsEmbeddedServer.get("auth") + ",");
		info.append("\"tsl_required\":" + (Boolean)NatsEmbeddedServer.get("ssl") + ",");
		info.append("\"max_payload\":" + (Integer)NatsEmbeddedServer.get("max_payload"));
		info.append("}" + CR_LF);
		
		sendCommand(info.toString().getBytes(), info.length());
	}
	
	private void parse() throws BufferUnderflowException {
		int index = pos + 4, rid = 0, start = pos + 4;
		byte[] buf = nUtil.getBuffer();
		
		for (; buf[index++] != 0xd;) {
			if (buf[index] == 0x20) {
				if (rid == 0) { param1 = new String(new String(buf, start, index-start)); }						
				else if (rid == 1) { param2 = new String(new String(buf, start, index-start)); }
				rid++;
				start = ++index;
			}
		}
		
		param3 = new String(buf, start, index-start-1);
		length = Integer.parseInt(param3);
	}		

	public void msg(String sid, ClientConnection dest) throws IOException {
		if (param1 == null) return;

		int offset = bytesCopy(cmd, 0, "MSG ");
		offset = bytesCopy(cmd, offset, param1);
		cmd[offset++] = 0x20; // SPC
		offset = bytesCopy(cmd, offset, sid);
		cmd[offset++] = 0x20; 
		if (param2 != null)  {
			offset = bytesCopy(cmd, offset, param2);
			cmd[offset++] = 0x20;
		}
		offset = bytesCopy(cmd, offset, param3);
		cmd[offset++] = 0xd; // CRLF
		cmd[offset++] = 0xa;
		offset = bytesCopy(cmd, offset, body);
		cmd[offset++] = 0xd; // CRLF
		cmd[offset++] = 0xa;

		dest.sendCommand(cmd, offset);
	}
	
	public int bytesCopy(byte[] b, int start, String data) {
		int end = start + data.length();
		for(int idx = 0; start < end; start++, idx++)
			b[start] = (byte)data.charAt(idx);

		return end;
	}
		
	private void payload() throws IOException, BufferUnderflowException {
		receiveBuffer.get(nUtil.getBuffer(), 0, length + 2);
		body = new String(nUtil.getBuffer(), 0, length);
		Subscription.message(this, param1);
		param2 = null;
		length = -1;
	}

	@Override
	public void sendPing(MsgHandler handler) throws IOException {
		sendCommand(PING_REQUEST, PING_REQUEST_LEN);		
	}	
}
