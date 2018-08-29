package org.brandao.entityfilemanager;

import java.io.IOException;

public interface DataWritter {

	void writeLong(long value) throws IOException;
	
	void writeInt(int value) throws IOException;

	void writeShort(short value) throws IOException;

	void writeByte(byte value) throws IOException;

	void writeChar(char value) throws IOException;
	
	void write(byte[] b) throws IOException;
	
	void write(byte[] b, int off, int len) throws IOException;
	
	void writeString(String value, int length) throws IOException;

	void writeString(String[] value, int length) throws IOException;
	
}
