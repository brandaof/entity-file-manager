package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.Lock;

public interface EntityFileAccess<T, R, H> {

	Class<T> getType();
	
	Class<R> getRawType();
	
	H getMetadata();
	
	EntityFileDataHandler<T,R,H> getEntityFileDataHandler();
	
	void setBatchLength(int value);

	int getBatchLength();
	
	Lock getLock();
	
	void reset() throws IOException;
	
	File getAbsoluteFile();
	
	String getAbsolutePath();
	
	String getName();
	
	void createNewFile() throws IOException;
	
	void open() throws IOException;
	
	void seek(long value) throws IOException;
	
	long getOffset() throws IOException;
	
	void batchWrite(T[] entities) throws IOException;
	
	void write(T value) throws IOException;
	
	void writeRaw(R value) throws IOException;

	void write(T[] b, int off, int len) throws IOException;
	
	void writeRaw(R[] b, int off, int len) throws IOException;
	
	void batchWriteRaw(R[] entities) throws IOException;
	
	T[] batchRead(int len) throws IOException;
	
	T read() throws IOException;
	
	R readRaw() throws IOException;
	
	R[] batchReadRaw(int len) throws IOException;
	
	int read(T[] b, int off, int len) throws IOException;
	
	int readRaw(R[] b, int off, int len) throws IOException;
	
	long length() throws IOException;
	
	void setLength(long value) throws IOException;
	
	boolean exists();
	
	void flush() throws IOException ;
	
	void close() throws IOException;
	
	void delete() throws IOException;
	
}
