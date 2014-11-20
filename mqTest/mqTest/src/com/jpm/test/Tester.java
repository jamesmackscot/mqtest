package com.jpm.test;

import java.io.IOException;

import com.jpm.queue.Log;
import com.jpm.queue.Message;
import com.jpm.queue.MessageQueue;
import com.jpm.queue.Scheduler;

public class Tester {

	public static void sleepSecs(long t) {
		// For test simulate a slight delay in adding more messages
		try {
			System.out.println("Sleeping for "+t+" seconds");
			Thread.sleep(t*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Log.setupLogging();
		Log.logger.info("Starting tester");
		if (args.length ==0 || (args.length > 0 && args[0].toLowerCase().equals("help"))) {
			//Spit out help
			System.out.println("HELP");
			System.out.println("----");
			System.out.println("Enter a list of commands seperated by a space using the following rules");
			System.out.println("Mx - send a message with the groupID of x, e.g. M2 for message for groupID 2");
			System.out.println("Tx - send a termination message with the groupID of x, e.g. M2 for message for groupID 2");
			System.out.println("Sx - Sleep for x seconds, e.g. S2 to sleep for 2 seconds before doing anything else");
			System.out.println("Rx - Change the available resources to x seconds, e.g. R2 to set 2 resourses");
			System.out.println("C - Cleanup the cancelled queue history");
			System.out.println("P - Cleanup the processed queue history");
			System.out.println("F - Cleanup the terminated message queue history");
			System.out.println("Q - Cleanup the queue messages");
			System.exit(0);
		} else {
			int i=0;
			Scheduler s = Scheduler.getInstance();
			for (String a : args) {
				try {
					System.out.println(a.substring(1));
						int l = 0;
					if (a.length() > 1) {
						l = Integer.valueOf(a.substring(1)).intValue();
					}
					char b = a.toLowerCase().charAt(0);
					switch (b) {
						case 'm' : s.addToQueue(new Message("test"+i++, l));break;
						case 't' : s.addToQueue(new Message("test"+i++, l,true));break;
						case 's' : sleepSecs(l);break;
						case 'r' : s.setResources(l);break;
						case 'c' : System.out.println("Cleaning the cancelled queue history");MessageQueue.getQueue().clearCancelledGroupHistory();break;
						case 'p' : System.out.println("Cleaning the processed queue history");MessageQueue.getQueue().clearProcessedGroupHistory();break;
						case 'f' : System.out.println("Cleaning the terminated message queue history");MessageQueue.getQueue().clearCompletedGroupHistory();break;
						case 'q' : System.out.println("Cleaning the queue messages");MessageQueue.getQueue().clear();break;
						default : System.out.println("Invalid argument "+a);
					}
				} catch (Exception e) {
					System.out.println("Invalid argument -"+a);
				}
			}
		}
		Log.logger.info("Test completed");
		Log.logger.info("press ENTER to call System.exit() and run the shutdown routine.");
try {
	System.in.read();
} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
System.exit(0);
	}

}