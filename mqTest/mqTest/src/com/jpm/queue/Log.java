package com.jpm.queue;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Log {
	
	public final static Logger logger = Logger.getLogger("Scheduler");
	
	private static FileHandler fh = null;
	
	public static void setupLogging() {
		//So different classes can call this and it will be harmless
		if (null ==fh) {
			try {
				fh = new FileHandler("scheduler.log");
				fh.setFormatter(new SimpleFormatter());
				logger.addHandler(fh);
				logger.setLevel(Level.INFO);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}