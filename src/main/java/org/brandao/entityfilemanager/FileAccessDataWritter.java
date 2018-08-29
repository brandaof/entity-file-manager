package org.brandao.entityfilemanager;

import java.io.IOException;

public class FileAccessDataWritter 
	implements DataWritter{

	private FileAccess fa;
	
	public FileAccessDataWritter(FileAccess fa){
		this.fa = fa;
	}
	
	public void writeLong(long value) throws IOException {
		fa.writeLong(value);
	}

	public void writeInt(int value) throws IOException {
		fa.writeInt(value);
	}

	public void writeShort(short value) throws IOException {
		fa.writeShort(value);
	}

	public void writeByte(byte value) throws IOException {
		fa.writeByte(value);
	}

	public void writeChar(char value) throws IOException {
		fa.writeChar(value);
	}

	public void write(byte[] b) throws IOException {
		fa.write(b);
	}

	public void write(byte[] b, int off, int len) throws IOException {
		fa.write(b, off, len);
	}

	public void writeString(String value, int length) throws IOException {
		fa.writeString(value, length);
	}

	public void writeString(String[] value, int length) throws IOException {
		fa.writeString(value, length);
	}

}
