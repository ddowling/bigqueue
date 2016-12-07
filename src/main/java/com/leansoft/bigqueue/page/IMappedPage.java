package com.leansoft.bigqueue.page;

import java.nio.ByteBuffer;

/**
 * Memory mapped page file ADT
 * 
 * @author bulldog
 *
 */
public interface IMappedPage {
	
	/**
	 * Get a thread local copy of the mapped page buffer
	 * 
	 * @param position start position(relative to the start position of source mapped page buffer) of the thread local buffer
	 * @return a byte buffer with specific position as start position.
	 */
	ByteBuffer getLocal(int position);
	
	/**
	 * Get data from a thread local copy of the mapped page buffer
	 * 
	 * @param position start position(relative to the start position of source mapped page buffer) of the thread local buffer
	 * @param length the length to fetch
	 * @return byte data
	 */
	public byte[] getLocal(int position, int length);
	
	/**
	 * Check if this mapped page has been closed or not
	 * 
	 * @return
	 */
	boolean isClosed();
	
	/**
	 * Set if the mapped page has been changed or not
	 * 
	 * @param dirty
	 */
	void setDirty(boolean dirty);
	
	/**
	 * The back page file name of the mapped page
	 * 
	 * @return
	 */
	String getPageFile();
	
	/**
	 * The index of the mapped page
	 * 
	 * @return the index
	 */
	long getPageIndex();
	
	/**
	 * Persist any changes to disk
	 */
	public void flush();

	/**
	 * Methods to get, set and increment longs in the page. This is used to efficiently maintain
	 * queue head and tail counters. Eventually this will use the java.unsafe mechanism to make these operations
	 * atomic and thus safe to use across process boundaries.
	 *
	 * Get a long with the given offset.
	 * @param position to read. Note position must be 8 byte aligned
	 * @return the value
	 */
	public long getLongAtPosition(int position);

	/**
	 * Set long into position in the mapped page
	 * @param position to write into
	 * @param value to set
	 */
	public void setLongAtPosition(int position, long value);

	/**
	 * Increment the long value at the given position.
	 *
	 * @param position to update
	 * @return the original value before the increment was applied
	 */
	public long incrementLongAtPosition(int position);
}
