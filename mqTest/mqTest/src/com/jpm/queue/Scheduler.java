package com.jpm.queue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Properties;

public class Scheduler implements Runnable{
	
	//Scheduler will be a singleton so we only have one running at a time
	private static Scheduler s = null;
	Thread queueThread;
	//Object to use for waiting and notifying
	Object waitGuard = new Object();
	//Configure the number of available resources from a gateway.properties file
	private static String propertyFile = "scheduler.properties";
	private String gateWayClass = "";
	private String comparatorClass = null;
	private int resources = 0;
	private Integer usedResources = 0;
	//First thing we need to define is a sorted list to hold the items that are queued
	private MessageQueue queue = null;
	//We need to store a reference to the previous groupID's that have been processed
	private String processGroupOrder = "";
	
	
	//Block external object creation
	private Scheduler() {
		Log.setupLogging();
		if (loadProperties()) {
			Log.logger.info("Scheduler configured to manage ["+resources+"] resources");
		} else {
			//Error?
			Log.logger.info("Could not load properties file...exiting");
			System.exit(1);
		}
		Log.logger.info("Loading queue");
		queue = MessageQueue.getQueue();
		if (queue == null) {
			System.exit(1);
		}
		if (null != comparatorClass) {
			//Change the comparator class in the queue
			queue.setComparator(comparatorClass);
		}
		//Register a controlled shutdown method to backup all data
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() { 
		    	Scheduler.shutdown();
		    	queue.shutdown();
		    }
		 });
		
		//Start the queue processing engine on its own thread
		queueThread = new Thread(this);
		queueThread.setName("QueueProcessor");
		queueThread.start();
	}
	
	protected static void shutdown() {
		//Kill the queue thread
		Log.logger.info("Shutdown the queue scheduler");
		if (null != s) {
			s.queueThread.interrupt();
		}
	}

	//Singleton creator
	public static Scheduler getInstance() {
		if (s == null) {
			synchronized(Scheduler.class){
				if (s == null)
					s = new Scheduler();
			}
		}
		return s;
	}

	private boolean loadProperties() {
		try {
			//Locate the ini file by getting its full path
			URL url = getClass().getClassLoader().getResource(propertyFile);
			if (null != url) {
				String filePath = url.getPath();
				if (filePath.toUpperCase().startsWith("FILE:/")) {
					//Strip off the actual file path
					filePath = filePath.substring(6);
				}
				File file = new File(filePath);
				//Check we can find the file
				if (file.isFile()) {
					InputStream is = new FileInputStream(file);
					Properties properties = new Properties();
					properties.load(is);
					if (properties.containsKey("resources")) {
						//This will exception if value is not an int
						resources = Integer.valueOf(properties.getProperty("resources"));
						gateWayClass=properties.getProperty("gateway.class");
						comparatorClass=properties.getProperty("comparator.class");
						String queueStore = properties.getProperty("backup.file");
						if (null != queueStore) {
							//Set it and restore any backup that might be there
							MessageQueue.setBackupFile(queueStore);
						}
					}
					is.close();
					Log.logger.info("Loaded properties file[" + propertyFile + "]");
					return true;
				} else {
					Log.logger.info("Error finding properties file[" + propertyFile + "] in path["+file.getParent()+"]");
				}
			} else {
				Log.logger.info("Error finding properties file[" + propertyFile + "]");
			}
		} catch (Exception e) {
			Log.logger.info("Error loading properties file[" + propertyFile + "] Error[" + e.getMessage() + "]");
			e.printStackTrace();
		}
		return false;
	}
	
	
	//Method to add a new Message to the queue
	public boolean addToQueue(Message msg) {
		Log.logger.info("Adding a new message for processing.");
		boolean ret = queue.add(msg);
		if (ret) {
			//Wake up queue processing thread
			synchronized (s) {
				s.notify();
			}
		}
		return ret;
	}

	public boolean cancelMessageGroup(long groupID) {
		return queue.cancelMessageGroup(groupID);
	}
	
	@Override
	public void run() {
		//This loop needs to check the queue and available resources and add when available
		synchronized (s) {
				//Loop indefinitely
				for (;;) {
					try {
					if (getUsedResources() < resources && !queue.isEmpty()) {
						//We can process one or more item
						//Get the first item in the set and remove it from the set
						Message msg = null;
						//Queue type does not require synchronisation
						//synchronized (queue) {
							msg = queue.pollFirst();
						//}
						processGroupOrder +="("+msg.getGroupID()+")";
						Log.logger.info("Order ["+processGroupOrder+"]");
						//Mark a resource as used
						decUsedResources();
						Log.logger.info("Processing message with data["+msg.getData()+"] groupID["+msg.getGroupID()+"] added on["+msg.getTime()+"]");
						sendToGateway(msg);
					} else {
						Log.logger.info("Idle waiting for notify()");
						s.wait();
					}
					} catch (InterruptedException ex) {
						//Should add a check for shutdown each loop to cleanly exit
						Log.logger.info("Interrupted");
					}
				}
		}		
	}
	
	//This call uses reflection to invoke the Gateway send method as it is referenced by a property
	public void sendToGateway(Message msg) {
        try {
            //Get the class
            Class<?> c = Class.forName(gateWayClass);
            Object[] args = new Object[] { msg };
            Method method = c.getMethod("send", Message.class);
            method.invoke(null, args);
        } catch (Exception e) {
            Log.logger.severe("Cannot invoke Gateway.send message using gateway class ["+gateWayClass+"]");
            System.exit(1);
        }
	}	


	public int getUsedResources() {
		synchronized (usedResources) {
			Log.logger.info("Used "+usedResources);
			return usedResources;
		}
	}

	public void decUsedResources() {
		synchronized (usedResources) {
			Log.logger.info("Used "+usedResources);
			usedResources++;
		}
	}

	public void incUsedResources() {
		synchronized (usedResources) {
			Log.logger.info("Used "+usedResources);
			usedResources--;
		}
	}
		
	public static void main(String[] args) {
		Log.setupLogging();
		Log.logger.info("Starting Scheduler");
		Scheduler.getInstance();
		Log.logger.info("Scheduler started and waiting for work");
	}

	public void setResources(int resources) {
		Log.logger.info("Setting external resourses to "+resources);
		this.resources = resources;
	}
	
}