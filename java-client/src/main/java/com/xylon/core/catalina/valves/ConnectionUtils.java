package com.xylon.core.catalina.valves;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;


public final class ConnectionUtils {
	private static final Log log = LogFactory.getLog(ConnectionUtils.class);
	private static final char NL = '\n';
	public static final String MAGIC_V2 = "  V1";

	protected static String command(String cmd, String... params){
		StringBuilder commandBuilder = new StringBuilder(cmd);
		for(String param : params){
			commandBuilder.append(" ");
			commandBuilder.append(param);
		}
		commandBuilder.append(NL);
		String out = commandBuilder.toString();
		log.debug("Cmd: {}" +  out);
		return out;
	}
	
	public static String subscribe(String topic, String channel, String shortId, String longId){
		// TODO: check validity of topc and channel
		return command("SUB", topic, channel, shortId, longId);
	}
	
	public static String get(String msg){
		// TODO: check validity of topc and channel
		return command("GET", msg);
	}
	
	public static String ready(int count){
		return command("RDY", Integer.toString(count));
	}
	
	public static String finish(byte[] id){
		return command("FIN", new String(id));
	}
	
	public static String requeue(byte[] id, int timeMs){
		return command("REQ", new String(id), Integer.toString(timeMs));
	}
	
	public static String nop(){
		return command("NOP");
	}
}
