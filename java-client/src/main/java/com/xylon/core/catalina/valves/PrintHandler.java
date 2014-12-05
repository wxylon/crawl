/**
* Copyright(c) 2014-2014, wxylon@gmailcom. All Rights Reserved.
*/
package com.xylon.core.catalina.valves;

/**
 * @author wxylon@gmail.com
 * @date Dec 4, 2014
 */
public class PrintHandler implements SyncResponseHandler{
	private String channelName;
	
	public PrintHandler(String channelName){
		this.channelName = channelName;
	}

	public boolean handleMessage(Message msg) throws NSQException {
		System.out.println(channelName + "-->Received: " + new String(msg.getBody()));
		return true;
	}
}

