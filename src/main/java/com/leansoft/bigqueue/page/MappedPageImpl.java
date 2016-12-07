package com.leansoft.bigqueue.page;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class MappedPageImpl implements IMappedPage, Closeable {
	
	private final static Logger logger = LoggerFactory.getLogger(MappedPageImpl.class);
	
	private ThreadLocalByteBuffer threadLocalBuffer;
	private volatile boolean dirty = false;
	private volatile boolean closed = false;
	private String pageFile;
	private long index;
	
	public MappedPageImpl(MappedByteBuffer mbb, String pageFile, long index) {
		this.threadLocalBuffer = new ThreadLocalByteBuffer(mbb);
		this.pageFile = pageFile;
		this.index = index;
	}
	
	public void close() throws IOException {
		synchronized(this) {
			if (closed) return;

			flush();
			
			MappedByteBuffer srcBuf = (MappedByteBuffer)threadLocalBuffer.getSourceBuffer();
			unmap(srcBuf);
			
			this.threadLocalBuffer = null; // hint GC
			
			closed = true;
			if (logger.isDebugEnabled()) {
				logger.debug("Mapped page for " + this.pageFile + " was just unmapped and closed.");
			}
		}
	}
	
	@Override
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
	@Override
	public void flush() {
		synchronized(this) {
			if (closed) return;
			if (dirty) {
				MappedByteBuffer srcBuf = (MappedByteBuffer)threadLocalBuffer.getSourceBuffer();
				srcBuf.force(); // flush the changes
				dirty = false;
				if (logger.isDebugEnabled()) {
					logger.debug("Mapped page for " + this.pageFile + " was just flushed.");
				}
			}
		}
	}

	public byte[] getLocal(int position, int length) {
		ByteBuffer buf = this.getLocal(position);
		byte[] data = new byte[length];
		buf.get(data);
		return data;
	}
	
	@Override
	public ByteBuffer getLocal(int position) {
		ByteBuffer buf = this.threadLocalBuffer.get();
		buf.position(position);
		return buf;
	}
	
	private static void unmap(MappedByteBuffer buffer)
	{
		Cleaner.clean(buffer);
	}
	
    /**
     * Helper class allowing to clean direct buffers.
     */
    private static class Cleaner {
        public static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
            }
            CLEAN_SUPPORTED = v;
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(ByteBuffer buffer) {
    		if (buffer == null) return;
            if (CLEAN_SUPPORTED && buffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(buffer);
                    directBufferCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    // silently ignore exception
                }
            }
        }
    }
    
    private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
    	private ByteBuffer _src;
    	
    	public ThreadLocalByteBuffer(ByteBuffer src) {
    		_src = src;
    	}
    	
    	public ByteBuffer getSourceBuffer() {
    		return _src;
    	}
    	
    	@Override
    	protected synchronized ByteBuffer initialValue() {
    		ByteBuffer dup = _src.duplicate();
    		return dup;
    	}
    }

	@Override
	public boolean isClosed() {
		return closed;
	}
	
	public String toString() {
		return "Mapped page for " + this.pageFile + ", index = " + this.index + ".";
	}

	@Override
	public String getPageFile() {
		return this.pageFile;
	}

	@Override
	public long getPageIndex() {
		return this.index;
	}

	// Use the machanism in http://mishadoff.com/blog/java-magic-part-4-sun-dot-misc-dot-unsafe/ to get a handle to
	// the unsafe instance
	static Unsafe getUnsafe() {
    	try {
			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);

			return (Unsafe) f.get(null);
		}
		catch(Exception e) {
			return null;
		}
	}

	private static final Unsafe UNSAFE = getUnsafe();

	@Override
	public long getLongAtPosition(int  position) {
		if (UNSAFE == null) {
			// Use an inefficient access method if Unsafe is not available
			ByteBuffer buf = threadLocalBuffer.get();
			return buf.getLong(position);
		} else {
			ByteBuffer buf = threadLocalBuffer.getSourceBuffer();
			long address = ((DirectBuffer)buf).address();
			return UNSAFE.getLongVolatile(null, address + position);
		}
	}

    @Override
	public void setLongAtPosition(int position, long value) {
		if (UNSAFE == null) {
			// Use an inefficient access method if Unsafe is not availavble
			ByteBuffer buf = threadLocalBuffer.get();
			buf.putLong(position, value);
		} else {
			ByteBuffer buf = threadLocalBuffer.getSourceBuffer();
			long address = ((DirectBuffer)buf).address();
			UNSAFE.putLongVolatile(null, address + position, value);
		}
	}

    @Override
	public long incrementLongAtPosition(int position) {
		if (UNSAFE == null) {
			// Use an inefficient and race condition prone access method if Unsafe is not available
			ByteBuffer buf = threadLocalBuffer.get();
			long orig_value = buf.getLong(position);
			buf.putLong(position, orig_value + 1);
			return orig_value;
		} else {
			ByteBuffer buf = threadLocalBuffer.getSourceBuffer();
			long address = ((DirectBuffer)buf).address();

			while(true) {
				long orig_value = UNSAFE.getLongVolatile(null, address + position);
				long new_value = orig_value + 1;

				if (UNSAFE.compareAndSwapLong(null, address + position, orig_value, new_value))
					return orig_value;
			}
		}
	}
}
