package com.leansoft.bigqueue;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.io.IOException;

/**
 * Queue ADT
 * 
 * @author bulldog
 *
 */
public interface IBigQueue extends Closeable {

	/**
	 * Determines whether a queue is empty
	 * 
	 * @return ture if empty, false otherwise
	 */
	public boolean isEmpty() throws IOException;
	
	/**
	 * Adds an item at the back of a queue
	 * 
	 * @param data to be enqueued data
	 * @throws IOException exception throws if there is any IO error during enqueue operation.
	 */
	public void enqueue(byte[] data) throws IOException;
	
	/**
	 * Retrieves and removes the front of a queue
	 * 
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during dequeue operation.
	 */
	public byte[] dequeue() throws IOException;

    /**
     * Retrieves a Future which will complete if new Items where enqued.
     *
     * Use this method to retrieve a future where to register as Listener instead of repeatedly polling the queues state.
     * On complete this future contains the result of the dequeue operation. Hence the item was automatically removed from the queue.
     *
     * @return a ListenableFuture which completes with the first entry if items are ready to be dequeued.
     */
    public ListenableFuture<byte[]> dequeueAsync();

	
	/**
	 * Removes all items of a queue, this will empty the queue and delete all back data files.
	 * 
	 * @throws IOException exception throws if there is any IO error during remove operation.
	 */
	public void removeAll() throws IOException;

	/**
	 * Remove n items from a queue in one operation. This operation is useful if the queue is being examined using the
	 * peek() or peekAtOffset() methods and a batch of previously examined entries needs to be removed.
	 *
	 * @param n items to remove
	 * @throws IOException if there is any IO error during the remove operation.
	 */
	public void removeN(long n) throws IOException;

	/**
	 * Retrieves the item at the front of a queue
	 * 
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	public byte[] peek()  throws IOException;

	/**
	 * Retrieves the item at an offset along a queue. This method is used together with removeN to implement
	 * a simple transaction model on a queue where a number of entries can be sequentially examined on the queue
	 * using peekAtOffset() and then the actual dequeue can be committed by calling removeN() or rolled back by
	 * resetting the offset back to 0.
	 *
	 * @param offset along the queue to examine
	 * @return data from the queue at the given offset or null if the offset is outside the queue bounds
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	public byte[] peekAtOffset(long offset)  throws IOException;

    /**
     * Retrieves the item at the front of a queue asynchronously.
     * On complete the value set in this future is the result of the peek operation. Hence the item remains at the front of the list.
     *
     * @return a future containing the first item if available. You may register as listener at this future to be informed if a new item arrives.
     */
    public ListenableFuture<byte[]> peekAsync();

    /**
     * apply an implementation of a ItemIterator interface for each queue item
     *
     * @param iterator
     * @throws IOException
     */
    public void applyForEach(ItemIterator iterator) throws IOException;
	
	/**
	 * Delete all used data files to free disk space.
	 * 
	 * BigQueue will persist enqueued data in disk files, these data files will remain even after
	 * the data in them has been dequeued later, so your application is responsible to periodically call
	 * this method to delete all used data files and free disk space.
	 * 
	 * @throws IOException exception throws if there is any IO error during gc operation.
	 */
	public void gc() throws IOException;
	
	/**
	 * Force to persist current state of the queue, 
	 * 
	 * normally, you don't need to flush explicitly since:
	 * 1.) BigQueue will automatically flush a cached page when it is replaced out,
	 * 2.) BigQueue uses memory mapped file technology internally, and the OS will flush the changes even your process crashes,
	 * 
	 * call this periodically only if you need transactional reliability and you are aware of the cost to performance.
	 */
	public void flush();
	
	/**
	 * Total number of items available in the queue.
	 * @return total number
	 */
	public long size() throws IOException;
	
	/**
	 * Item iterator interface
	 */
	public static interface ItemIterator {
        /**
         * Method to be executed for each queue item
         *
         * @param item queue item
         * @throws IOException
         */
        public void forEach(byte[] item) throws IOException;
    }
}
