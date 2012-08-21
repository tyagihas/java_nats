# Java_Nats

A Java client for the [NATS messaging system](https://github.com/derekcollison/nats).

[![Build Status](https://secure.travis-ci.org/derekcollison/node_nats.png)](http://travis-ci.org/derekcollison/node_nats)

## Basic Usage

```javascript
import java.util.Properties;
import nats.Session;
...
Session session = Session.connect(new Properties());
session.start();

// Simple Publisher
session.publish("foo", "Hello World!", null, null);

// Simple Subscriber
session.subscribe("foo", session.new EventHandler() {
	public void execute(String msg) {
		System.out.println("Received a message: " + msg);
	}
});

// Unsubscribing
Integer sid = session.subscribe("foo", session.new EventHandler() {
	public void execute(String msg) {
		System.out.println("Received a message: " + msg);
	}
});		
session.unsubscribe(sid);

// Requests
sid = session.request("help", session.new EventHandler() {
	public void execute(String response) {
		System.out.println("Got a response for help : " + reponse);
	}
});
		
// Replies
session.subscribe("help", session.new RequestEventHandler() {
	public void execute(String request, String replyTo) {
		try {
			session.publish(replyTo, "I can help!");
		} catch (IOException e) {
			e.printStackTrace();
		}				
	}
});		

session.flush();
session.stop();
```

## Wildcard Subcriptions

```javascript
// "*" matches any token, at any level of the subject.
session.subscribe("foo.*.baz", session.new EventHandler() {
	public void execute(String msg, String reply, String subject) {
		System.out.println("Received a message on [" + subject + "] : " + msg);
	}
});

session.subscribe("foo.bar.*", session.new EventHandler() {
	public void execute(String msg, String reply, String subject) {
		System.out.println("Received a message on [" + subject + "] : " + msg);
	}
});

session.subscribe("*.bar.*", session.new EventHandler() {
	public void execute(String msg, String reply, String subject) {
		System.out.println("Received a message on [" + subject + "] : " + msg);
	}
});

// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
session.subscribe("foo.>", session.new EventHandler() {
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
session.subscribe(args[0], opt, session.new EventHandler() {
	public void execute(String msg) {
		System.out.println("Received update : " + msg);
	}
});
```

## Advanced Usage

```javascript
// Publish with closure, callback fires when server has processed the message
session.publish("foo", "You done?", session.new EventHandler() {
	public void execute() {
		System.out.println("Message processed!");
	}
});

// Timeouts for subscriptions
Integer sid = session.subscribe("foo", session.new EventHandler() {
	int received = 0;
	public void execute() {
		received++;
	}
});
session.timeout(sid, TIMEOUT_IN_SECS, session.new EventHandler() {
	public void execute() {
		timeout_recv = true;
	}
});

// Timeout unless a certain number of messages have been received
Properties opt = new Properties();
opt.put("expected", new Integer(2));
session.timeout(sid, 10, opt, session.new EventHandler() {
	public void execute(Object o) {
		timeout_recv = true;
	}
});

// Auto-unsubscribe after MAX_WANTED messages received
session.unsubscribe(sid, MAX_WANTED)

// Multiple connections
session1.subscribe("test", session.new EventHandler() {
	public void execute(String msg) {
    	System.out.println("received : " + msg);
    }
});

// Form second connection to send message on
Session session2 = Session.connect(new Properties());
session2.start(session2.new EventHandler() {
	public void execute(Object o) {
		Session session = (Session)o;
		try {
			session.publish("test", "Hello World!");
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


