package org.brandao.entityfilemanager;

import java.io.IOException;
import java.io.InputStream;

public class DataReaderInputStream 
	extends InputStream
	implements DataReader{

	private InputStream in;
	
	private DataReader reader;

	public DataReaderInputStream(InputStream in){
		this.in = in;
		this.reader = new AbstractDataReader() {
			
			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				return DataReaderInputStream.this.in.read(b, off, len);
			}
			
		};
	}
	
	public long readLong() throws IOException {
		return reader.readLong();
	}

	public int readInt() throws IOException {
		return reader.readInt();
	}

	public short readShort() throws IOException {
		return reader.readShort();
	}

	public byte readByte() throws IOException {
		return reader.readByte();
	}

	public char readChar() throws IOException {
		return reader.readChar();
	}

	public String readString(int length) throws IOException {
		return reader.readString(length);
	}

	public String[] readStrings(int length) throws IOException {
		return reader.readStrings(length);
	}

	@Override
	public int read() throws IOException {
		return in.read();
	}

	public int read(byte[] b, int off, int len) throws IOException {
		return in.read(b, off, len);
	}
	
}
