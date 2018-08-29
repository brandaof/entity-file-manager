package org.brandao.entityfilemanager;

import java.io.IOException;

public interface EntityFileDataHandler<T, R, H> {

	void writeMetaData(DataWritter stream, H value) throws IOException;
	
	H readMetaData(DataReader srteam) throws IOException;
	
	void writeEOF(DataWritter stream) throws IOException;
	
	void write(DataWritter stream, T entity) throws IOException;
	
	void writeRaw(DataWritter stream, R entity) throws IOException;
	
	T read(DataReader stream) throws IOException;
	
	R readRaw(DataReader stream) throws IOException;
	
	long getFirstPointer();

	int getHeaderLength();
	
	int getRecordLength();
	
	int getEOFLength();
	
	int getFirstRecord();

	Class<T> getType();
	
	Class<R> getRawType();
	
}
