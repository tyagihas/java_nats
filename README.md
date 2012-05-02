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
	public void execute(Object o) {
		System.out.println("Received a message: " + (String)o);
	}
});

// Unsubscribing
Integer sid = session.subscribe("foo", session.new EventHandler() {
	public void execute(Object o) {}
});		
session.unsubscribe(sid);

// Requests
sid = session.request("help", session.new EventHandler() {
	public void execute(Object o) {
		System.out.println("Got a response for help : " + o);
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


