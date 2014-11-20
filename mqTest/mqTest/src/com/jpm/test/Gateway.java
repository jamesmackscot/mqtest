package com.jpm.test;

import java.util.Random;

import com.jpm.queue.Log;
import com.jpm.queue.Message;

public class Gateway {

	//Make this test gateway static so this will never be called
	private Gateway() {
	}
	
	// The send function is called by the queue when a resource becomes available
	public static void send(Message msg) {
		//Make this async so create a new thread and launch it to process the msg and return
		Thread t = new Thread(new Resource(msg));
		t.start();
	}
	
	private static class Resource implements Runnable {
		
		private Message msg;
		
		public Resource(Message msg) {
			this.msg=msg;
		}
		
		@Override
		public void run() {
			// Wait based on a random number of seconds between 1 - 10 and then call the Message.completed() function 
			int i = new Random().nextInt(10000); //0 - 10 secs in ms
			try {
				Log.logger.info("Resource working for "+i/1000+"seconds");
				Thread.sleep(i);
			} catch (InterruptedException e) {
				//This should only happen in process shutdown
				e.printStackTrace();
			}
			msg.completed();
		}
		//Thread will stop
	}
}