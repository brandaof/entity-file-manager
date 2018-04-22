package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

public interface EntityFileAccess<T, R> {

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
	
	void batchWrite(List<T> entities) throws IOException;
	
	void write(T value) throws IOException;
	
	void writeRawEntity(R value) throws IOException;

	void batchWriteRawEntity(List<R> entities) throws IOException;
	
	List<T> batchRead(long value) throws IOException;
	
	T read() throws IOException;
	
	R readRawEntity() throws IOException;
	
	List<R> batchReadRawEntity(long value) throws IOException;
	
	long length() throws IOException;
	
	void setLength(long value) throws IOException;
	
	boolean exists();
	
	void close() throws IOException;
	
}
