package org.brandao.entityfilemanager;

import java.io.IOException;

public abstract class AbstractDataReader 
	implements DataReader{

	private byte[] buffer = new byte[8];
	
	public long readLong() throws IOException {
		read(buffer, 0, 8);
		return 
			 (long)buffer[0]       & 0xffL              | 
			((long)buffer[1] << 8  & 0xff00L)           | 
			((long)buffer[2] << 16 & 0xff0000L)         | 
			((long)buffer[3] << 24 & 0xff000000L)       |
			((long)buffer[4] << 32 & 0xff00000000L)     | 
			((long)buffer[5] << 40 & 0xff0000000000L)   | 
			((long)buffer[6] << 48 & 0xff000000000000L) | 
			((long)buffer[7] << 56 & 0xff00000000000000L);
	}

	public int readInt() throws IOException {
		read(buffer, 0, 4);
		return 
			 (int)buffer[0]       & 0xff      | 
			((int)buffer[1] << 8  & 0xff00)   | 
			((int)buffer[2] << 16 & 0xff0000) | 
			((int)buffer[3] << 24 & 0xff000000);
	}

	public short readShort() throws IOException {
		read(buffer, 0, 2);
		return (short)(
				 buffer[0]       & 0xff |
				(buffer[1] << 8  & 0xff00)
		);
	}

	public byte readByte() throws IOException {
		read(buffer, 0, 1);
		return buffer[0];
	}

	public char readChar() throws IOException {
		read(buffer, 0, 1);
		return (char) buffer[0];
	}
	
	public int read(byte[] b) throws IOException{
		return read(b);
	}
	
	public abstract int read(byte[] b, int off, int len) throws IOException;
	
	public String readString(int length) throws IOException{
		byte[] result = new byte[length];
		read(result, 0, length);
		return DataUtil.bytesToString(result, 0);
	}

	public String[] readStrings(int length) throws IOException {
		byte[] buffer = new byte[length];
		read(buffer, 0, length);
		return DataUtil.bytesToStrings(buffer, 0);
	}
	
}