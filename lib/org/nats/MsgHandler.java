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
		} catch (Exception e) {
			e.printStackTrace();
		}			
	}
}

