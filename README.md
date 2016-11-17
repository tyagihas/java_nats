# Java_Nats

Java client library and server testing module for [NATS messaging system](http://nats.io).

## Supported Platforms

java_nats currently supports following Java Platforms :
- Java Platform, Standard Edition 8 (Java SE 8)

## Getting Started

Add dependency to Maven pom.xml

```xml
<dependency>
	<groupId>com.github.tyagihas</groupId>
	<artifactId>java_nats</artifactId>
	<version>0.7.0</version>
</dependency>
```

## Basic Usage

```java
import java.util.Properties;
import org.nats.*;
...
Connection conn = Connection.connect(new Properties());

// Simple Publisher
conn.publish("foo", "Hello World!", null, null);

// Simple Subscriber
conn.subscribe("foo", new MsgHandler() {
	public void execute(String msg) {
		System.out.println("Received a message: " + msg);
	}
});

// Unsubscribing
Integer sid = conn.subscribe("foo", new MsgHandler() {
	public void execute(String msg) {
		System.out.println("Received a message: " + msg);
	}
});		
conn.unsubscribe(sid);

// Requests
sid = conn.request("help", new MsgHandler() {
	public void execute(String response) {
		System.out.println("Got a response for help : " + reponse);
	}
});

// Replies
conn.subscribe("help", new MsgHandler() {
	public void execute(String request, String replyTo) {
		try {
			conn.publish(replyTo, "I can help!");
		} catch (IOException e) {
			e.printStackTrace();
		}				
	}
});		

conn.close();
```

## Wildcard Subcriptions

```java
// "*" matches any token, at any level of the subject.
conn.subscribe("foo.*.baz", new MsgHandler() {
	public void execute(String msg, String reply, String subject) {
		System.out.println("Received a message on [" + subject + "] : " + msg);
	}
});

conn.subscribe("foo.bar.*", new MsgHandler() {
	public void execute(String msg, String reply, String subject) {
		System.out.println("Received a message on [" + subject + "] : " + msg);
	}
});

conn.subscribe("*.bar.*", new MsgHandler() {
	public void execute(String msg, String reply, String subject) {
		System.out.println("Received a message on [" + subject + "] : " + msg);
	}
});

// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
conn.subscribe("foo.>", new MsgHandler() {
	public void execute(String msg, String reply, String subject) {
		System.out.println("Received a message on [" + subject + "] : " + msg);
	}
});
```

## Queues Groups

```java
// All subscriptions with the same queue name will form a queue group
// Each message will be delivered to only one subscriber per queue group, queuing semantics
// You can have as many queue groups as you wish
// Normal subscribers will continue to work as expected.
Properties opt = new Properties();
opt.setProperty("queue", "job.workers");
conn.subscribe(args[0], opt, new MsgHandler() {
	public void execute(String msg) {
		System.out.println("Received update : " + msg);
	}
});
```

## Clustered Usage

```java
Properties opts = new Properties();
opts.put("servers", "nats://user1:pass1@server1:4242,nats://user2:pass2@server2:4243");

Connection conn = Connection.connect(opts);
conn.publish("hello", "world");

```

## Binary messages

```java
byte[] bmsg = "Hello World!".getBytes();

conn1.subscribe(new MsgHandler() {
	// Receiving a message as a byte array
	public void execute(byte[] msg) {
		System.out.println("Received : " + new String(msg));
	}
});
conn1.flush();

// Publishing a byte array message
conn2.publish("test", bmsg);
```

## Advanced Usage

```java
// Publish with closure, callback fires when server has processed the message
conn.publish("foo", "You done?", new MsgHandler() {
	public void execute() {
		System.out.println("Message processed!");
	}
});

// Timeouts for subscriptions
Integer sid = conn.subscribe("foo", new MsgHandler() {
	int received = 0;
	public void execute() {
		received++;
	}
});
conn.timeout(sid, TIMEOUT_IN_SECS, new MsgHandler() {
	public void execute() {
		timeout_recv = true;
	}
});

// Timeout unless a certain number of messages have been received
Properties opt = new Properties();
opt.put("expected", new Integer(2));
conn.timeout(sid, 10, opt, new MsgHandler() {
	public void execute(Object o) {
		timeout_recv = true;
	}
});

// Auto-unsubscribe after MAX_WANTED messages received
conn.unsubscribe(sid, MAX_WANTED)

// Multiple connections
conn1.subscribe("test", new MsgHandler() {
	public void execute(String msg) {
    	System.out.println("received : " + msg);
    }
});

// Form second connection to send message on
Connection conn2 = Connection.connect(new Properties(), new MsgHandler() {
	public void execute(Object o) {
		Connection conn = (Connection)o;
		try {
			conn.publish("test", "Hello World!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
});
```

## TLS

* Use "keytool" to create TrustStore and KeyStore (if necessary) or specify existing ones in next step.

* Set properties to configure TrustStore and KeyStore.

```java
Properties props = new Properties();
props.put("truststore", "./truststore");
props.put("truststore_pass", "password");
// KeyStore is used only when tlsverify is set on the server.
props.put("keystore", "./keystore");
props.put("keystore_pass", "password");

// Automatically detect if TLS is configured on the server.
Connection conn = Connection.connect(props);
```

* It may be required to set TLS "timeout" parameter longer than default.

See examples and benchmarks for more information.

## Nats Embedded Server (beta)

Start a Nats Embedded Server from Java application.
```java
Properties props = new Properties();
props.setProperty("port", "4222");

NatsEmbeddedServer server = new NatsEmbeddedServer(props);
```

Start a Nats Embedded Server from command line. Assume CLASSPATH contains java_nats and slf4j jar files.
```bash
% java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -Xms1G -Xmx1G org.nats.server.NatsEmbeddedServer
```

Nats Embedded Server is intended for dev and unit testing and not for production use. Following features haven't been implemented in Nats Embedded Server.
* TLS
* Clustering

## License

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
