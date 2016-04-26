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

package org.nats.common;

import java.nio.ByteBuffer;

public class NatsUtil {
	private byte[] buf;
	private int pos;
	
	public NatsUtil() {
		buf = new byte[Constants.INIT_BUFFER_SIZE];
		for(int i = 0; i < buf.length; i++) {
			buf[i] = '\0';
		}
	}
	
	public int readNextOp(int start, ByteBuffer buffer) {
		pos = start;
		int limit = buffer.limit();
		
		for(; buffer.position() < limit; pos++) {
			buf[pos] = buffer.get();
			if ((pos > 0) && (buf[pos] == '\n') && (buf[pos-1] == '\r')) { return 0; }
			
		}
		return pos;
	}
	
	public String getOp() {
		return new String(buf, 0, pos-1);
	}

	public boolean compare(byte[] dest, int length) {
		for(int i = 0; i < length; i++)
			if (buf[i] != dest[i])
				return false;
		return true;
	}	
	
	public byte[] getBuffer() {
		return buf;
	}

	public ByteBuffer expandBuffer(ByteBuffer src) {
		ByteBuffer dest = ByteBuffer.allocateDirect(src.capacity() * 2);
		int bytes = src.limit();
		int i = 0;
		src.flip();
		while(src.hasRemaining() && i < bytes) {
			dest.put(src.get());
			i++;
		}
		return dest;
	}
	
}
