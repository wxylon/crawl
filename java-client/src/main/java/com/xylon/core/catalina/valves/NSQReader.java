package com.xylon.core.catalina.valves;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;


public abstract class NSQReader {
	private static final Log log = LogFactory.getLog(NSQReader.class);
	protected String topic;
	protected String channel;
	protected String shortHostname;
	protected String hostname;
	
	protected ExecutorService executor;
	
	protected Class<? extends Connection> connClass;
	Connection conn;
	
	public void init(String topic, String channel){
		this.executor = Executors.newSingleThreadExecutor(); // TODO can be passed by caller
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			this.hostname = "unknown.host";
		}
		String[] hostParts = this.hostname.split("\\.");
		this.shortHostname = hostParts[0];
		
		this.connClass = BasicConnection.class; // TODO can be passed by caller

		// register action for shutdown
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run(){
				shutdown();
			}
		});
	}
	
	public void shutdown(){
		log.info("NSQReader received shutdown signal, shutting down connections");
        this.executor.shutdown();
	}
	
	protected abstract Runnable makeRunnableFromMessage(Message msg);
		
	public void addMessageForProcessing(Message msg){
		this.executor.execute(this.makeRunnableFromMessage(msg));
	}
	
	public void connectToNsqd(String address, int port) throws NSQException{
		try {
			conn = this.connClass.newInstance();
		} catch (InstantiationException e) {
			throw new NSQException("Connection implementation must have a default constructor");
		} catch (IllegalAccessException e) {
			throw new NSQException("Connection implementation's default constructor must be visible");
		}
		conn.init(address, port, this);
		conn.connect();
		conn.send(ConnectionUtils.subscribe("123121", "123121", "123121", "123121"));
		conn.readForever();
		
		try {
			while(true){
				Thread.currentThread().sleep(1000);
				conn.send(ConnectionUtils.get("234"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String toString(){
		return "Reader<" + this.topic + ", " + this.channel + ">";
	}

	public String getTopic() {
		return topic;
	}
}
