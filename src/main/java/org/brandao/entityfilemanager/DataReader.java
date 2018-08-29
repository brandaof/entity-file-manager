package org.brandao.entityfilemanager;

import java.io.IOException;

public interface DataReader {

	long readLong() throws IOException;
	
	int readInt() throws IOException;

	short readShort() throws IOException;

	byte readByte() throws IOException;

	char readChar() throws IOException;
	
	int read(byte[] b) throws IOException;
	
	int read(byte[] b, int off, int len) throws IOException;
	
	String readString(int length) throws IOException;

	String[] readStrings(int length) throws IOException;
	
}
