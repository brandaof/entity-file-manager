package org.brandao.entityfilemanager;

import java.io.IOException;

public interface EntityFileDataHandler<T, R> {

	void writeMetaData(DataOutputStream stream) throws IOException;
	
	void readMetaData(DataInputStream srteam) throws IOException;
	
	void writeEOF(DataOutputStream stream) throws IOException;
	
	void write(DataOutputStream stream, T entity) throws IOException;
	
	void writeRaw(DataOutputStream stream, R entity) throws IOException;
	
	T read(DataInputStream stream) throws IOException;
	
	R readRaw(DataInputStream stream) throws IOException;
	
	Class<T> getType();
	
}
