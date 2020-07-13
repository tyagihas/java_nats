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

package org.nats;

/**
 * Message handler can be passed to various operations and invoked when the operation is processed by the server.
 * @author Teppei Yagihashi
 */
public abstract class MsgHandler {
	private static final Class<?>[] ARITY0 = {};
	private static final Class<?>[] ARITY1 = {String.class};
	private static final Class<?>[] ARITY2 = {String.class, String.class};
	private static final Class<?>[] ARITY3 = {String.class, String.class, String.class};
	private static final Class<?>[] OBJ = {Object.class};
	private static final Class<?>[] ARITYB1 = {byte[].class};
	private static final Class<?>[] ARITYB2 = {byte[].class, String.class};
	private static final Class<?>[] ARITYB3 = {byte[].class, String.class, String.class};

	private static final String className = "org.nats.MsgHandler";
	public Thread caller;
	public int arity;
	
	public MsgHandler() {
		verifyArity();
	}
	
	public void execute() {}
	public void execute(String msg) {}
	public void execute(String msg, String reply) {}
	public void execute(String msg, String reply, String subject) {}		
	public void execute(Object o) {}
	public void execute(byte[] msg) {}
	public void execute(byte[] msg, String reply) {}
	public void execute(byte[] msg, String reply, String subject) {}		
	
	private void verifyArity() {
		try {
			if (!getClass().getMethod("execute", ARITY0).getDeclaringClass().getName().equals(className))
				arity = 0;
			else if (!getClass().getMethod("execute", ARITY1).getDeclaringClass().getName().equals(className))
				arity = 1;
			else if (!getClass().getMethod("execute", ARITY2).getDeclaringClass().getName().equals(className))
				arity = 2;
			else if (!getClass().getMethod("execute", ARITY3).getDeclaringClass().getName().equals(className))
				arity = 3;
			else if (!getClass().getMethod("execute", OBJ).getDeclaringClass().getName().equals(className))
				arity = -1;
			else if (!getClass().getMethod("execute", ARITYB1).getDeclaringClass().getName().equals(className))
				arity = 11;
			else if (!getClass().getMethod("execute", ARITYB2).getDeclaringClass().getName().equals(className))
				arity = 12;
			else if (!getClass().getMethod("execute", ARITYB3).getDeclaringClass().getName().equals(className))
				arity = 13;
		} catch (Exception e) {
			e.printStackTrace();
		}			
	}
}

