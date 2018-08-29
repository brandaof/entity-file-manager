package org.brandao.entityfilemanager;

import java.io.IOException;
import java.io.OutputStream;

public class DataWritterOutputStream
	extends OutputStream
	implements DataWritter {

	private OutputStream stream;
	
	private DataWritter writter;
	
	public DataWritterOutputStream(OutputStream stream){
		this.stream = stream;
		this.writter = new AbstractDataWritter() {
			
			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				DataWritterOutputStream.this.stream.write(b, off, len);
			}
		};
		
	}

	public void writeLong(long value) throws IOException{
		writter.writeLong(value);
	}
	
	public void writeInt(int value) throws IOException{
		writter.writeInt(value);
	}

	public void writeShort(short value) throws IOException{
		writter.writeShort(value);
	}

	public void writeByte(byte value) throws IOException{
		writter.writeByte(value);
	}

	public void writeChar(char value) throws IOException{
		writter.writeChar(value);
	}
	
	public void write(byte[] b, int off, int len) throws IOException{
		writter.write(b, off, len);
	}
	
	public void write(byte[] b) throws IOException{
		writter.write(b);
	}
	
	public void write(int b) throws IOException{
		writter.writeByte((byte)b);
	}
	
	public void writeString(String value, int length) throws IOException{
		writter.writeString(value, length);
	}

	public void writeString(String[] value, int length) throws IOException{
		writter.writeString(value, length);
	}
	
}
