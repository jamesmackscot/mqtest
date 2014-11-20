package com.jpm.queue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MessageQueue extends ArrayList<Message> {

	/**
	 * Special case TreeSet that will reorder on removal of a message that differs in groupID from the last one removed.
	 */
	private static final long serialVersionUID = 681702890102712268L;
	//Overload the pollFirst() function that we use to cause a reorder event
	//We need to store a reference to the previous groupID's that have been processed
	public Set<Long> processedGroupID = null; //This is public so alternative Comparators may be used 
	//In order to remove interleaving, we need to ensure the last groupID processed receives the highest priority
	public long lastGroupID = -1;
	
	private boolean reorder = false;
	private Comparator<Message> comparator = null;
	private boolean shutdown = false;
	//Default location to recover and backup data
	private static String backupFile = "backup.bin";
	//Hold reference as its a singleton
	private static MessageQueue queue = null;
	//Set to hold a list of cancelled groupID's so if new ones are added to the queue they will be rejected
	public Set<Long> cancelledGroups = null;
	public Set<Long> completedGroups = null;

	
	
	//Need to define the constructor
	private MessageQueue() {
		super();
		comparator = new MessageComparator();
		processedGroupID = new HashSet<Long>();
		cancelledGroups = new HashSet<Long>();
		completedGroups = new HashSet<Long>();
	}

	@Override
	public boolean add(Message msg) {
		boolean ret = false;
		if (!shutdown) {
			//Check if a message being added is in a group that is already terminated
			synchronized (completedGroups) {
				if (completedGroups.contains(msg.getGroupID())) {
					Log.logger.severe("ERROR - Adding a Message that is from a group["+msg.getGroupID()+"] marked as completed");
					return false;
				} else if (msg.lastMessage) {
					//Mark this message as the last one in the group so no more may be added
					completedGroups.add(msg.getGroupID());
				}
			}
			//Check if the group is marked as cancelled
			synchronized (cancelledGroups) {
				if (cancelledGroups.contains(msg.getGroupID())) {
					Log.logger.info("Not processing Message as its group["+msg.getGroupID()+"] is marked as cancelled");
					return false;
				}
			}
			//Sync on the queue although it will limit one add or remove at a time
			synchronized (this) {
				//add it to the queue
				ret = super.add(msg);
				Log.logger.info("Queue size:"+queue.size());
			}
			if (ret)
				reorder = true;
		}
		return ret;
	}
	
	//Flush the queue
	public void clear() {
		synchronized (this) {
			super.clear();
		}
	}
	
	public void clearCancelledGroupHistory() {
		synchronized (cancelledGroups) {
			cancelledGroups.clear();
		}
	}
	
	public void clearCompletedGroupHistory() {
		synchronized (completedGroups) {
			completedGroups.clear();
		}
	}
	
	public void clearProcessedGroupHistory() {
		synchronized (processedGroupID) {
			processedGroupID.clear();
			lastGroupID = -1;
		}
	}
	
	public void setComparator(String compClass) {
		if (null != compClass) {
	        try {
	            //Get the class
	            @SuppressWarnings("unchecked")
				Class<Comparator<Message>> c = (Class<Comparator<Message>>) Class.forName(compClass);
	            if (null != c) {
	            	setComparator(c.newInstance());
	            	
	            }
	        } catch (Exception e) {
	            Log.logger.severe("Cannot invoke new Comparator for messages from class ["+compClass+"]");
	            
	        }
		}
	}
	
	public void setComparator(Comparator<Message> c) {
		if (null != c) {
			Log.logger.info("Changed the default Comparator to ["+c.getClass().getName()+"]");
			comparator = c;
			reorder=true;
		}
	}
	
	//Check the queue is in the correct order then return the first message
	public Message pollFirst() {
		//Should check if anything has changed to make this inconsistent before reordering
		if (reorder) {
			Log.logger.fine("Triggering reordering of the queued messages. lastGroupID["+lastGroupID+"]");
			Collections.sort(this,comparator);
			Log.logger.fine("Reordering complete");
		}
		Message el;
		synchronized (this) {
			el = remove(0);
		}
		synchronized (processedGroupID) {
			if (el.getGroupID() != lastGroupID) {
				lastGroupID=el.getGroupID();
				addProcessedGroupID(lastGroupID);
				reorder = true;
			}
		}
		return el;
	}
	
	//Checks if the group is already cancelled, if not then process the cancellation and cleanup the queue accordingly
	public boolean cancelMessageGroup(long groupID) {
		boolean added = false;
		synchronized (cancelledGroups) {
			//Add returns true if the element has been added
			added = cancelledGroups.add(groupID);
		}
		if (added) {
			Log.logger.info("Cancelled groupID ["+groupID+"]");
			//Remove any remaining messages from the queue
			synchronized (this) {
				removeMessageGroupMembers(groupID);
			}
		}
		return added;
	}
	
	//Removes all the group messages from the queue
	public void removeMessageGroupMembers(long groupID) {
		//Create a temp array
		List<Message> r = new ArrayList<Message>();
		for (Message msg : this) {
			if (msg.getGroupID() == groupID) {
				r.add(msg);
			}
		}
		//Now remove them from the queue
		Log.logger.info("Removing ["+r.size()+"] messages from the queue with groupID["+groupID+"]");
		removeAll(r);
		reorder = true;
		r.clear();
	}
	
	public void addProcessedGroupID(long groupID) {
			processedGroupID.add(groupID);
	}
	
	protected void shutdown() {
		Log.logger.info("Shutting down the message queue");
		//Block any new entries
		shutdown = true;
		//Serialise any data to disk
		backupQueue();
	}

	public static String getBackupFile() {
		return backupFile;
	}

	public static void setBackupFile(String backupFile) {
		MessageQueue.backupFile = backupFile;
	}

	//Backup entire object in one go so it is straightforward to recover
	public synchronized boolean backupQueue() {
		boolean ok = false;
		//Drain the queue to a list, then requeue the tasks in a new list.
		Log.logger.info("Draining queue to list object");
		File file = new File(backupFile);
		if (file.exists()) {
			Log.logger.info("Deleting existing queue file");
			file.delete();
		}
		try {
			file.createNewFile();
			if (file.isFile()) {
				Log.logger.info("Serialising queue objects to file[" + file.getName() + "]");
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
				out.writeObject(this); 
				out.close();
				ok = true;
			} else {
				Log.logger.severe("Queue file creation failed.");
			}
		} catch (Exception e) {
			Log.logger.severe("Exception thrown whilst serialising queue tasks [" + e.getMessage() + "]");
		}
		return ok;
	}
	
	//This will try to restore a backup queue first then create one if not found
	public static MessageQueue getQueue() {
		if (queue == null) {
			synchronized(Scheduler.class){
				if (queue == null) {
					//Try to restore queue object from serialized backup
					File file = new File(backupFile);
					Log.logger.info("Trying to load backup queue file");
					if (file.exists() && file.isFile()) {
						try {
							Log.logger.info("File found, loading...");
							ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
							queue = (MessageQueue) in.readObject();
							in.close();
							//Important...reset the queue to working again!
							queue.shutdown = false;
							if (file.delete()) {
								Log.logger.info("Deleted queue backup file");
							} else {
								Log.logger.severe("Error deleting queue backup file");
							}
							if (!queue.isEmpty()) {
								Log.logger.info("There are " + queue.size() + " items in the restored queue");
							} else {
								Log.logger.info("There were no items in the restored queue");
							}
							Log.logger.info("Queue now primed with backed up tasks.");
							return queue;
						} catch (FileNotFoundException e) {
							Log.logger.info("FilenotFound = " + e.getMessage());
						} catch (IOException e) {
							Log.logger.severe( "IOException thrown whilst loading queue tasks [" + e.getMessage() + "]");
							if (file.delete()) {
								Log.logger.info("Deleted queue backup file");
							}
						} catch (ClassNotFoundException e) {
							Log.logger.severe( "ClassNotFoundException thrown whilst importing queue tasks [" + e.getMessage() + "]");
						}
					} else {
						Log.logger.info("No backup queue file found");
					}
					Log.logger.info("Creating new queue");
					queue = new MessageQueue();
				}
			}
		}
		return queue;
	}
	
}