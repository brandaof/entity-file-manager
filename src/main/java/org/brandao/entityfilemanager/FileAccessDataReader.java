package org.brandao.entityfilemanager;

import java.io.IOException;

public class FileAccessDataReader 
	implements DataReader{

	private FileAccess fa;

	public FileAccessDataReader(FileAccess fa){
		this.fa = fa;
	}

	public long readLong() throws IOException {
		return fa.readLong();
	}

	public int readInt() throws IOException {
		return fa.readInt();
	}

	public short readShort() throws IOException {
		return fa.readShort();
	}

	public byte readByte() throws IOException {
		return fa.readByte();
	}

	public char readChar() throws IOException {
		return fa.readChar();
	}

	public int read(byte[] b) throws IOException {
		return fa.read(b);
	}

	public int read(byte[] b, int off, int len) throws IOException {
		return fa.read(b, off, len);
	}

	public String readString(int length) throws IOException {
		return fa.readString(length);
	}

	public String[] readStrings(int length) throws IOException{
		return fa.readStrings(length);
	}
	
}
