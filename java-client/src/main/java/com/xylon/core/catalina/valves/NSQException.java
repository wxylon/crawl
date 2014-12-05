package com.xylon.core.catalina.valves;

public class NSQException extends Exception {

	public NSQException(Throwable t) {
		super(t);
	}

	public NSQException(String string) {
		super(string);
	}

}
