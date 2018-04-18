package org.brandao.entityfilemanager;

import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStream {

	private byte[] buffer = new byte[8];
	
	private OutputStream stream;
	
	public DataOutputStream(OutputStream stream){
		this.stream = stream;
	}

	public void writeLong(long value) throws IOException{
		buffer[0] = (byte)(value & 0xffL); 
		buffer[1] = (byte)(value >> 8  & 0xffL); 
		buffer[2] = (byte)(value >> 16 & 0xffL); 
		buffer[3] = (byte)(value >> 24 & 0xffL);
		buffer[4] = (byte)(value >> 32 & 0xffL); 
		buffer[5] = (byte)(value >> 40 & 0xffL); 
		buffer[6] = (byte)(value >> 48 & 0xffL); 
		buffer[7] = (byte)(value >> 56 & 0xffL);
		this.stream.write(this.buffer, 0, 8);
	}
	
	public void writeInt(int value) throws IOException{
		buffer[0] = (byte)(value & 0xff);
		buffer[1] = (byte)((value >> 8) & 0xff);
		buffer[2] = (byte)((value >> 16) & 0xff);
		buffer[3] = (byte)((value >> 24) & 0xff);
		this.stream.write(this.buffer, 0, 4);
	}

	public void writeShort(short value) throws IOException{
		buffer[0] = (byte)(value & 0xff);
		buffer[1] = (byte)((value >> 8) & 0xff);
		this.stream.write(this.buffer, 0, 2);
	}

	public void writeByte(byte value) throws IOException{
		buffer[0] = value;
		this.stream.write(this.buffer, 0, 1);
	}

	public void writeChar(char value) throws IOException{
		buffer[0] = (byte)value;
		this.stream.write(this.buffer, 0, 1);
	}
	
	public void write(byte[] b, int off, int len) throws IOException{
		this.stream.write(b, off, len);
	}
	
	public void write(byte[] b) throws IOException{
		this.stream.write(b);
	}
	
	public void write(int b) throws IOException{
		this.stream.write(b);
	}
	
	public void writeString(String value, int length) throws IOException{
		byte[] result = DataUtil.stringToBytes(value, length);
		this.stream.write(result, 0, length);
	}

	public void writeString(String[] value, int length) throws IOException{
		byte[] buffer = DataUtil.stringsToBytes(value, length);
		this.stream.write(buffer, 0, length);
	}
	
}
