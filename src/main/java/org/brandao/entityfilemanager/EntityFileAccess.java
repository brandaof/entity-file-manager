package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

public interface EntityFileAccess<T, R> {

	Class<T> getType();
	
	int getRecordLength();
	
	int getFirstRecord();
	
	EntityFileDataHandler<T> getEntityFileDataHandler();
	
	void setBatchLength(int value);

	int getBatchLength();
	
	ReadWriteLock getLock();
	
	File getAbsoluteFile();
	
	String getAbsolutePath();
	
	String getName();
	
	void createNewFile() throws IOException;
	
	void open() throws IOException;
	
	void seek(long value) throws IOException;
	
	long getOffset() throws IOException;
	
	void batchWrite(T[] entities) throws IOException;
	
	void write(T value) throws IOException;
	
	void writeRawEntity(R value) throws IOException;

	void batchWriteRawEntity(R[] entities) throws IOException;
	
	T[] batchRead(int len) throws IOException;
	
	T read() throws IOException;
	
	R readRawEntity() throws IOException;
	
	R[] batchReadRawEntity(int len) throws IOException;
	
	long length() throws IOException;
	
	void setLength(long value) throws IOException;
	
	boolean exists();
	
	void close() throws IOException;
	
}
