package org.brandao.entityfilemanager;

import java.io.IOException;

public interface EntityFileDataHandler<T> {

	void writeMetaData(DataOutputStream stream) throws IOException;
	
	void readMetaData(DataInputStream srteam) throws IOException;
	
	void writeEOF(DataOutputStream stream) throws IOException;
	
	void write(DataOutputStream stream, T entity) throws IOException;
	
	T read(DataInputStream stream) throws IOException;
	
	Class<T> getType();
	
}
