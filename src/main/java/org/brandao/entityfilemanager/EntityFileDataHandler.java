package org.brandao.entityfilemanager;

import java.io.IOException;

public interface EntityFileDataHandler<T, R, H> {

	void writeMetaData(DataOutputStream stream, H value) throws IOException;
	
	H readMetaData(DataInputStream srteam) throws IOException;
	
	void writeEOF(DataOutputStream stream) throws IOException;
	
	void write(DataOutputStream stream, T entity) throws IOException;
	
	void writeRaw(DataOutputStream stream, R entity) throws IOException;
	
	T read(DataInputStream stream) throws IOException;
	
	R readRaw(DataInputStream stream) throws IOException;
	
	int getHeaderLength();
	
	int getRecordLength();
	
	int getEOFLength();
	
	long getFirstRecord();

	Class<T> getType();
	
	Class<R> getRawType();
	
}
