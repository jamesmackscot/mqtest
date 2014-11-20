package com.jpm.queue;

import java.io.Serializable;

public class Message implements Comparable<Message>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4234476049750103353L;
	//Assume for now the message takes a string of data as input
	private String data;
	//Assume the groupID is a long integer
	private long groupID;
	//Capture the added time so that it can be used by the sorting algorithm
	private long time;
	public boolean lastMessage = false;

	//Creation of a new message object requires the message data and groupID
	public Message(String data, long groupID) {
		this.data=data;
		this.groupID=groupID;
		time = System.currentTimeMillis();
	}
	
	public Message(String data, long groupID, boolean lastMessage) {
		this.data=data;
		this.groupID=groupID;
		time = System.currentTimeMillis();
		this.lastMessage=lastMessage;
	}
	
	public long getGroupID() {
		return groupID;
	}

	public void setGroupID(long groupID) {
		this.groupID = groupID;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public void completed() {
		Log.logger.info("Message completed in ["+(System.currentTimeMillis()-time)/1000+"] seconds");
		//Need to trigger the queue to recognise this is now complete and free up for another message to be queued
		Scheduler s = Scheduler.getInstance();
		s.incUsedResources();
		Log.logger.info("Notfiying queue thread");
		synchronized (s) {
			s.notify();
		}
	}

	public long getTime() {
		return time;
	}

	@Override
	public int compareTo(Message arg0) {
		Log.logger.info("Should not be calling this!");
		return ("Data:"+data+"group:"+groupID+"t:"+time).compareTo(("Data:"+arg0.data+"group:"+arg0.groupID+"t:"+arg0.time));
	}
	
	public boolean equals(Message m) {
		return (m.hashCode()) == hashCode();
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return ("Data:"+data+"group:"+groupID+"t:"+time).hashCode();
	}
	
}