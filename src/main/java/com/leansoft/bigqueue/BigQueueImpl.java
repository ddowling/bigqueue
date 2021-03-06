package com.leansoft.bigqueue;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.leansoft.bigqueue.page.IMappedPage;
import com.leansoft.bigqueue.page.IMappedPageFactory;
import com.leansoft.bigqueue.page.MappedPageFactoryImpl;


/**
 * A big, fast and persistent queue implementation.
 * <p/>
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 *
 * @author bulldog
 */
public class BigQueueImpl implements IBigQueue {

    final IBigArray innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";

    // factory for queue front index page management(acquire, release, cache)
    IMappedPageFactory queueFrontIndexPageFactory;

    // locks for queue front write management
    final Lock queueFrontWriteLock = new ReentrantLock();

    // lock for dequeueFuture access
    private final Object futureLock = new Object();
    private SettableFuture<byte[]> dequeueFuture;
    private SettableFuture<byte[]> peekFuture;

    /**
     * A big, fast and persistent queue implementation,
     * use default back data page size, see {@link BigArrayImpl#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName) throws IOException {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big, fast and persistent queue implementation.
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName, int pageSize) throws IOException {
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize);

        // the ttl does not matter here since queue front index page is always cached
        this.queueFrontIndexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                ((BigArrayImpl) innerArray).getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER,
                10 * 1000/*does not matter*/);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return this.getQueueFrontIndex() == this.innerArray.getHeadIndex();
    }

    @Override
    public void enqueue(byte[] data) throws IOException {
        this.innerArray.append(data);

        this.completeFutures();
    }


    @Override
    public byte[] dequeue() throws IOException {
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty())
                return null;

            // Set queueFrontIndex to current queue front and THEN increment
            long queueFrontIndex = this.incrementQueueFrontIndex();

            return this.innerArray.get(queueFrontIndex);
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public ListenableFuture<byte[]> dequeueAsync() {
        this.initializeDequeueFutureIfNecessary();
        return dequeueFuture;
    }



    @Override
    public void removeAll() throws IOException {
        try {
            queueFrontWriteLock.lock();
            this.innerArray.removeAll();
            this.setQueueFrontIndex(0L);
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void removeN(long n) throws IOException {
        try {
            queueFrontWriteLock.lock();

            if (n < 0)
                return;

            long p = getQueueFrontIndex() + n;
            long head = this.innerArray.getHeadIndex();
            if (p > head)
                p = head;

            this.setQueueFrontIndex(p);

        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public byte[] peek() throws IOException {
        if (this.isEmpty()) {
            return null;
        }

        return this.innerArray.get(getQueueFrontIndex());
    }

    @Override
    public byte[] peekAtOffset(long offset)  throws IOException {
        long p = getQueueFrontIndex() + offset;
        if (offset < 0 || p >= this.innerArray.getHeadIndex())
            return null;
        else
            return this.innerArray.get(p);
    }

    private final static int QUEUE_FRONT_POSITION = 0;

    long getQueueFrontIndex() throws IOException {
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        return queueFrontIndexPage.getLongAtPosition(QUEUE_FRONT_POSITION);
    }

    private long incrementQueueFrontIndex() throws IOException {
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
        return queueFrontIndexPage.incrementLongAtPosition(QUEUE_FRONT_POSITION);
    }

    private void setQueueFrontIndex(long index) throws IOException {
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
        queueFrontIndexPage.setLongAtPosition(QUEUE_FRONT_POSITION, index);
    }

    @Override
    public ListenableFuture<byte[]> peekAsync() {
        this.initializePeekFutureIfNecessary();
        return peekFuture;
    }

    @Override
    public void applyForEach(ItemIterator iterator) throws IOException {
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty()) {
                return;
            }

            long index = this.getQueueFrontIndex();
            for (long i = index; i < this.innerArray.size(); i++) {
                iterator.forEach(this.innerArray.get(i));
            }
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (this.queueFrontIndexPageFactory != null) {
            this.queueFrontIndexPageFactory.releaseCachedPages();
        }


        synchronized (futureLock) {
            /* Cancel the future but don't interrupt running tasks
            because they might perform further work not refering to the queue
             */
            if (peekFuture != null) {
                peekFuture.cancel(false);
            }
            if (dequeueFuture != null) {
                dequeueFuture.cancel(false);
            }
        }

        this.innerArray.close();
    }

    @Override
    public void gc() throws IOException {
        long beforeIndex = this.getQueueFrontIndex();
        if (beforeIndex == 0L) { // wrap
            beforeIndex = Long.MAX_VALUE;
        } else {
            beforeIndex--;
        }
        try {
            this.innerArray.removeBeforeIndex(beforeIndex);
        } catch (IndexOutOfBoundsException ex) {
            // ignore
        }
    }

    @Override
    public void flush() {
        try {
            queueFrontWriteLock.lock();
            this.queueFrontIndexPageFactory.flush();
            this.innerArray.flush();
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public long size() throws IOException {
        long qFront = this.getQueueFrontIndex();
        long qRear = this.innerArray.getHeadIndex();
        if (qFront <= qRear) {
            return (qRear - qFront);
        } else {
            return Long.MAX_VALUE - qFront + 1 + qRear;
        }
    }


    /**
     * Completes the dequeue future
     */
    private void completeFutures() {
        synchronized (futureLock) {
            if (peekFuture != null && !peekFuture.isDone()) {
                try {
                    peekFuture.set(this.peek());
                } catch (IOException e) {
                    peekFuture.setException(e);
                }
            }
            if (dequeueFuture != null && !dequeueFuture.isDone()) {
                try {
                    dequeueFuture.set(this.dequeue());
                } catch (IOException e) {
                    dequeueFuture.setException(e);
                }
            }
        }
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializeDequeueFutureIfNecessary() {
        synchronized (futureLock) {
            if (dequeueFuture == null || dequeueFuture.isDone()) {
                dequeueFuture = SettableFuture.create();
            }
            try {
                if (!this.isEmpty())
                    dequeueFuture.set(this.dequeue());
            } catch (IOException e) {
                dequeueFuture.setException(e);
            }
        }
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializePeekFutureIfNecessary() {
        synchronized (futureLock) {
            if (peekFuture == null || peekFuture.isDone()) {
                peekFuture = SettableFuture.create();
            }
            try {
                if (!this.isEmpty())
                    peekFuture.set(this.peek());
            } catch (IOException e) {
                peekFuture.setException(e);
            }
        }
    }

}
