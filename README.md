# Java_Nats

A Java client for the [NATS messaging system](https://github.com/derekcollison/nats).

[![Build Status](https://secure.travis-ci.org/derekcollison/node_nats.png)](http://travis-ci.org/derekcollison/node_nats)

## Supported Platforms

```javascript
java_nats currently supports following Java Platforms :

- Java Platform, Standard Edition 6 (Java SE 6)
- Java Platform, Standard Edition 7 (Java SE 7)
```

## Getting Started

```javascript
Download source files and "cd" to java_nats root directory :
% cd <java_nats>

Compile:
% javac -d ./bin ./lib/org/nats/*.java
% export CLASSPATH=./bin
% javac -d ./bin ./benchmark/org/nats/benchmark/*.java
% javac -d ./bin ./examples/org/nats/examples/*.java

Run:
cd ./bin
% ./PubPerf.sh 100000 16
```

## Basic Usage

```javascript
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

```javascript
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

```javascript
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

## Advanced Usage

```javascript
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

See examples and benchmarks for more information..

## License

(The MIT License)

Copyright (c) 2011-2012 Teppei Yagihashi

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


