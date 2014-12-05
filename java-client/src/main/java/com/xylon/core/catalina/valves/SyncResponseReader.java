package com.xylon.core.catalina.valves;

public class SyncResponseReader extends NSQReader {
	
	private SyncResponseHandler handler;
	
	public SyncResponseReader(String topic, String channel, SyncResponseHandler handler) {
		super();
		this.handler = handler;
		this.init(topic, channel);
	}

	private class SyncResponseMessageRunnable implements Runnable {
		
		public SyncResponseMessageRunnable(Message msg) {
			super();
			this.msg = msg;
		}

		private Message msg;

		public void run() {
			try{
				handler.handleMessage(msg);
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}
	}

	@Override
	protected Runnable makeRunnableFromMessage(Message msg) {
		return new SyncResponseMessageRunnable(msg);
	}

}
