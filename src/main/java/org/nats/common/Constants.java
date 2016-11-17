/*
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
*/

package org.nats.common;

public class Constants {
	public static final String version = "0.7.0";
	
	public static final int DEFAULT_PORT = 4222;
	public static final String DEFAULT_URI = "nats://localhost:" + Integer.toString(DEFAULT_PORT);
	
	// Parser status
	public static final int AWAITING_CONTROL = 0;
	public static final int AWAITING_MSG_PAYLOAD = 1;
	// Connection status
	public static final int OPEN = 0;
	public static final int CLOSE = 1;
	public static final int RECONNECT = 2;

	// TLS parameters
	public static final String DEFAULT_KEYSTORE = "./keystore";
	public static final String DEFAULT_TRUSTSTORE = "./truststore";
	public static final String DEFAULT_PASSWORD = "password";
	public static final String TLS_REQUIRED = "tls_required";
	public static final String TLS_VERSION = "TLSv1.2";

	// Reconnect Parameters, 2 sec wait, 10 tries
	public static final int DEFAULT_RECONNECT_TIME_WAIT = 2*1000;
	public static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = 3;
	public static final int DEFAULT_PING_INTERVAL = 4*1000;

	public static final int MAX_PENDING_SIZE = 32768;
	public static final int INIT_BUFFER_SIZE = 1 * 1024 * 1024; // 1 Mb
	public static final int MAX_BUFFER_SIZE = 16 * 1024 * 1024; // 16 Mb
	public static final long REALLOCATION_THRESHOLD = 1 * 1000; // 1 second
		
	public static final String CR_LF = "\r\n";
	public static final int CR_LF_LEN = CR_LF.length();
	public static final String EMPTY = "";
	public static final String SPC = " ";
	public static final char SEP = '.';
	public static final String WC = "*";
	public static final String ARR = ">";

	// Protocol
	public static final byte[] PUB = "PUB".getBytes();
	public static final byte[] SUB = "SUB".getBytes();
	public static final byte[] UNSUB = "UNSUB".getBytes();
	public static final byte[] CONNECT = "CONNECT".getBytes();
	public static final byte[] MSG = "MSG".getBytes();
	public static final byte[] PONG = "PONG".getBytes();
	public static final byte[] PING = "PING".getBytes();
	public static final byte[] INFO = "INFO".getBytes();
	public static final byte[] ERR = "-ERR".getBytes();
	public static final byte[] OK = "+OK".getBytes();

	// Responses
	public static final byte[] PING_REQUEST = ("PING" + CR_LF).getBytes();
	public static final int PING_REQUEST_LEN = PING_REQUEST.length;
	public static final byte[] PONG_RESPONSE = ("PONG" + CR_LF).getBytes();
	public static final int PONG_RESPONSE_LEN = PONG_RESPONSE.length;

	// Embeded Server properties
	public static final boolean DEFAULT_AUTH = false;
	public static final boolean DEFAULT_SSL = false;
	public static final int DEFAULT_MAX_PAYLOAD = 1 * 1024 * 1024; // 1Mb

	// Server status
	public static final int START = 0;
	public static final int SHUTDOWN = 1;
}
