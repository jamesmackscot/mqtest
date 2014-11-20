package com.jpm.queue;

import java.io.Serializable;
import java.util.Comparator;

//Use a Comparator to sort the queue. This will auto re-sort when needed based on the groupID's
public class MessageComparator2 implements Comparator<Message>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4980519739955842627L;
	private MessageQueue queue = null;

	public int compare(Message m1, Message m2) {
		if (queue == null) {
			queue = MessageQueue.getQueue();			
		}
		Log.logger.info("MessageComparator2 Comparing 1 [" + m1.getData() + "] ["
				+ m1.getGroupID() + "] to 2 [" + m2.getData() + "] ["
				+ m2.getGroupID() + "]");
		// Ok going to be complex!
		// If the groupID's are different use them to order
		if (m1.getGroupID() != m2.getGroupID()) {
			// We are here if both groupID's are different
			// If one of the groupID's equals the last groupID used, return that
			// first
			// synchronized (processedGroupID) {
			if (m1.getGroupID() == queue.lastGroupID) {
				Log.logger.fine("Compare lastGroupID[" + queue.lastGroupID
						+ "] returning -1");
				return -1;
			}
			if (m2.getGroupID() == queue.lastGroupID) {
				Log.logger.fine("Compare lastGroupID[" + queue.lastGroupID
						+ "] returning 1");
				return 1;
			}
			// }
			// If one of the msgs groupID matches one already processed then
			// prioritise that first
			boolean f1, f2;
			// synchronized (processedGroupID) {
			f1 = queue.processedGroupID.contains(m1.getGroupID());
			f2 = queue.processedGroupID.contains(m2.getGroupID());
			// }
			if (f1 && !f2) {
				Log.logger.fine("Compare processedGroupID returning -1");
				return -1;
			}
			if (f2 && !f1) {
				Log.logger.fine("Compare processedGroupID returning 1");
				return 1;
			}
		}
		// We do not need to group the other ID's together as they will
		// automatically reorder
		// Based on the lastGroupID and processedGroupIDs changing so just order
		// by time.
		if (m1.getTime() < m2.getTime()) {
			Log.logger.fine("Compare time returning -1");
			return -1;
		}
		if (m1.getTime() > m2.getTime()) {
			Log.logger.fine("Compare time returning 1");
			return 1;
		}
		// Just use the data to keep it consistent
		return m1.getData().compareTo(m2.getData());
	}
}
