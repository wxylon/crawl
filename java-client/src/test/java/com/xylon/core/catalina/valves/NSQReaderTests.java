/**
* Copyright(c) 2014-2014, wxylon@gmailcom. All Rights Reserved.
*/

package com.xylon.core.catalina.valves;

import org.junit.Test;

/**
 * @author wxylon@gmail.com
 * @date Dec 4, 2014
 */
public class NSQReaderTests {
	
	@Test
	public void test() throws Exception{
		
		SyncResponseHandler handler = new PrintHandler("123");
		NSQReader nsqReader = new SyncResponseReader("123", "123", handler);
		try {
			nsqReader.connectToNsqd("127.0.0.1", 3002);
		} catch (NSQException e) {
			e.printStackTrace();
		}
		Thread.currentThread().join();
	}
}

