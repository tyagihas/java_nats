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
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
**/

package org.nats;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer; 
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext; 
import javax.net.ssl.SSLEngine; 
import javax.net.ssl.SSLEngineResult.HandshakeStatus; 
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED; 
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING; 
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;

import static org.nats.common.Constants.TLS_VERSION;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SecureSocketChannel provides TLS channel to Connection class when gnatsd is configured to use TLS.
 * During initialization, it detects whether client verification is required. 
 * @author Teppei Yagihashi
 */
public class SecureSocketChannel extends SocketChannel {
  private static Logger LOG = LoggerFactory.getLogger(SecureSocketChannel.class);

  private SocketChannel channel; 
  private SSLContext sslContext;
  private SSLEngine engine;
  private ByteBuffer netOut;
  private ByteBuffer netIn;
  private final int appBufferSize;

  public SecureSocketChannel(SocketChannel channel, Properties opts) throws SSLException {
    super(SelectorProvider.provider());

    try {
      LOG.debug("Start initializing SecureSocketChannel");
      Socket socket = channel.socket();
      KeyManager[] km = null;
      if (opts.getProperty("tls_verify").equals("true")) {
        LOG.debug("Require client verification");
        KeyStore kks = KeyStore.getInstance("JKS");
        char[] pass = opts.getProperty("keystore_pass").toCharArray();
        kks.load(new FileInputStream(opts.getProperty("keystore")), pass);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(kks, pass);
        km = kmf.getKeyManagers();
      }

      KeyStore tks = KeyStore.getInstance("JKS");
      tks.load(new FileInputStream(opts.getProperty("truststore")),
          opts.getProperty("truststore_pass").toCharArray());
      TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
      tmf.init(tks);

      sslContext = SSLContext.getInstance(TLS_VERSION);
      sslContext.init(km, tmf.getTrustManagers(), new SecureRandom());
      this.channel = channel;
      this.channel.configureBlocking(true); 

      engine = sslContext.createSSLEngine(socket.getInetAddress().getHostAddress(),
          socket.getPort());
      engine.setUseClientMode(true);

      SSLSession session = engine.getSession();
      appBufferSize = session.getApplicationBufferSize();
      int netBufferSize = session.getPacketBufferSize();

      netOut = ByteBuffer.allocateDirect(netBufferSize); 
      netIn = ByteBuffer.allocateDirect(netBufferSize);

      this.startHandShake();
    } catch(Exception e) {
      sslContext = null;
      throw new SSLException("Failed establishing TLS connection due to " + e.getMessage());
    }
  }

  public int getAppBufferSize() {
    return appBufferSize;
  }

  private void startHandShake() throws IOException {
    LOG.debug("Start TLS handshaking");
    ByteBuffer appIn = ByteBuffer.allocateDirect(appBufferSize);
    ByteBuffer appOut = ByteBuffer.allocateDirect(appBufferSize);
    engine.beginHandshake();
    HandshakeStatus status = engine.getHandshakeStatus();
    while (status != FINISHED && status != NOT_HANDSHAKING) {
      switch (status) {
      case NEED_TASK:
        runDelegatedTasks();
        break;
      case NEED_UNWRAP:
        doUnwrap(appIn);
        appIn.clear();
        break;
      case NEED_WRAP:
        doWrap(appOut);
        break;
      }
      status = engine.getHandshakeStatus();
    }
    appIn.clear();
    appOut.clear();
    LOG.debug("Finish TLS handshaking");
  }

  private HandshakeStatus runDelegatedTasks() {
    Runnable task = null;
    while ((task = engine.getDelegatedTask()) != null) {
      task.run();
    }
    return engine.getHandshakeStatus();
  }

  private int doWrap(ByteBuffer sendBuffer) throws IOException {
    int bytes = 0;

    Status status = engine.wrap(sendBuffer, netOut).getStatus();
    switch (status) {
    case OK:
      runDelegatedTasks();
      netOut.flip();
      while(netOut.hasRemaining()) {
        bytes += channel.write(netOut);
      }
      netOut.clear();
      break;
    case BUFFER_OVERFLOW:
      // TODO : rare situation, but should enlarge the net buffer.
      break;
    default:
      throw new IOException( "Got unexpected status " + status);
    }
    return bytes;
  }

  private int doUnwrap(ByteBuffer buffer) throws IOException {
    int bytes = 0;

    if ((bytes = channel.read(netIn)) < 0) {
      throw new IOException( "Failed to establish SSL socket connection." );
    }
    netIn.flip();

    Status status = null;
    do {
      status = engine.unwrap(netIn, buffer).getStatus();
      switch(status) {
      case OK:
        runDelegatedTasks();
        break;
      case BUFFER_OVERFLOW:
        // TODO : rare situation, but should enlarge the app buffer.
        break;
      case BUFFER_UNDERFLOW:
        netIn.compact();
        return bytes - netIn.remaining();
      default:
        throw new IOException( "Got unexpected status " + status );
      }
    }
    while (netIn.hasRemaining());
    netIn.compact();

    return bytes;
  } 

  @Override 
  public int read(ByteBuffer receiveBuffer) throws IOException {
    int bytes = doUnwrap(receiveBuffer);
    return bytes; 
  } 

  @Override 
  public int write(ByteBuffer sendBuffer) throws IOException {
    int bytes = doWrap(sendBuffer);
    return bytes;
  }

  public void terminate() throws IOException {
    channel.close();
    LOG.debug("Terminating TLS channel");
  }

  @Override
  public <T> T getOption(SocketOption<T> name) throws IOException {
    return null;
  }

  @Override
  public Set<SocketOption<?>> supportedOptions() {
    return null;
  }

  @Override
  public SocketChannel bind(SocketAddress local) throws IOException {
    return null;
  }

  @Override
  public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
    return null;
  }

  @Override
  public SocketChannel shutdownInput() throws IOException {
    channel.shutdownInput();
    return this;
  }

  @Override
  public SocketChannel shutdownOutput() throws IOException {
    channel.shutdownInput();
    return this;
  }

  @Override
  public Socket socket() {
    return channel.socket();
  }

  @Override
  public boolean connect(SocketAddress remote) throws IOException {
    return false;
  } 

  @Override
  public boolean isConnected() {
    return channel.isConnected();
  }

  @Override
  public boolean isConnectionPending() {
    return channel.isConnectionPending();
  }

  @Override
  public boolean finishConnect() throws IOException {
    return channel.finishConnect();
  }

  @Override
  public SocketAddress getRemoteAddress() throws IOException {
    return channel.getRemoteAddress();
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    return 0;
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    return 0;
  }

  @Override
  public SocketAddress getLocalAddress() throws IOException {
    return channel.getLocalAddress();
  }

  @Override
  protected void implCloseSelectableChannel() throws IOException { }

  @Override
  protected void implConfigureBlocking(boolean block) throws IOException { }	
}