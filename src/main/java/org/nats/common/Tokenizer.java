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

package org.nats.common;

import static org.nats.common.Constants.SEP;

public class Tokenizer {
	private String buf;
	private int pos;
	private int start;
	private boolean isNext;
	
	public void setToken(String buf) {
		this.buf = buf;
		start = 0;
		pos = -1;
	}
	
	public boolean next() {
		start = ++pos;
		isNext = false;
		for (; pos < buf.length(); pos++) {
			if (buf.charAt(pos) == SEP) { 
				isNext = true; 
				break;
			}
		}
		return isNext;
	}
	
	public boolean isNext() { return isNext; }
	public String getToken() { return buf.substring(start, pos); }
	
	public boolean compare(String token) {
		int i = 0;
		for (int p = start; p < pos; p++, i++) {
			if (buf.charAt(p) != token.charAt(i)) { return false; }
		}
		return true;
	}
}
