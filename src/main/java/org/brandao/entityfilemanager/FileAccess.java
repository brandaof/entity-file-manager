package org.brandao.entityfilemanager;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileAccess {

	private byte[] buffer = new byte[8];
	
	private BufferedOutputStream out;
	
	private BufferedInputStream in;
	
	private RandomAccessFile randomAccessFile;
	
	private File file;
	
	private long innerPointer;
	
	public FileAccess(File file, RandomAccessFile randomAccessFile) throws IOException{
		this(file, randomAccessFile, 8192, 8192);
	}
	
	public FileAccess(File file, RandomAccessFile randomAccessFile, int readCapacity, int writeCapacity) throws IOException{
		this.file             = file;
		this.randomAccessFile = randomAccessFile;
		this.out              = new BufferedOutputStream(writeCapacity, new FileOutputStream(this.randomAccessFile.getFD()));
		this.in               = new BufferedInputStream(readCapacity, new FileInputStream(this.randomAccessFile.getFD()));
		this.innerPointer     = 0;
	}

	public long readLong() throws IOException {
		
		int i = in.read(buffer, 0, 8);
		
		if(i > 0){
			innerPointer += i;
		}
		
		if(i != 8){
			throw new EOFException();
		}
		
		//this.randomAccessFile.readFully(buffer, 0, 8);
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

	public void writeLong(long value) throws IOException{
		buffer[0] = (byte)(value & 0xffL); 
		buffer[1] = (byte)(value >> 8  & 0xffL); 
		buffer[2] = (byte)(value >> 16 & 0xffL); 
		buffer[3] = (byte)(value >> 24 & 0xffL);
		buffer[4] = (byte)(value >> 32 & 0xffL); 
		buffer[5] = (byte)(value >> 40 & 0xffL); 
		buffer[6] = (byte)(value >> 48 & 0xffL); 
		buffer[7] = (byte)(value >> 56 & 0xffL);
		//this.randomAccessFile.write(this.buffer, 0, 8);
		out.write(buffer, 0, 8);
		innerPointer += 8;
		
	}
	
	public int readInt() throws IOException {
		
		int i = in.read(buffer, 0, 4);
		
		if(i > 0){
			innerPointer += i;
		}
		
		if(i != 4){
			throw new EOFException();
		}
		
		//this.randomAccessFile.readFully(buffer, 0, 4);
		return 
				 (int)buffer[0]       & 0xff      | 
				((int)buffer[1] << 8  & 0xff00)   | 
				((int)buffer[2] << 16 & 0xff0000) | 
				((int)buffer[3] << 24 & 0xff000000);
	}

	public void writeInt(int value) throws IOException{
		buffer[0] = (byte)(value & 0xff);
		buffer[1] = (byte)((value >> 8) & 0xff);
		buffer[2] = (byte)((value >> 16) & 0xff);
		buffer[3] = (byte)((value >> 24) & 0xff);
		//this.randomAccessFile.write(this.buffer, 0, 4);
		out.write(buffer, 0, 4);
		innerPointer += 4;
	}

	public short readShort() throws IOException {
		
		int i = in.read(buffer, 0, 2);
		
		if(i > 0){
			innerPointer += i;
		}
		
		if(i != 2){
			throw new EOFException();
		}
		
		//this.randomAccessFile.readFully(buffer, 0, 2);
		return (short)(
				 buffer[0]       & 0xff |
				(buffer[1] << 8  & 0xff00)
		);
	}

	public void writeShort(short value) throws IOException{
		buffer[0] = (byte)(value & 0xff);
		buffer[1] = (byte)((value >> 8) & 0xff);
		//this.randomAccessFile.write(this.buffer, 0, 2);
		out.write(buffer, 0, 2);
		innerPointer += 2;
	}

	public byte readByte() throws IOException {
		
		int i = in.read(buffer, 0, 1);
		
		if(i > 0){
			innerPointer += i;
		}
		
		if(i != 1){
			throw new EOFException();
		}
		
		//this.randomAccessFile.readFully(buffer, 0, 1);
		return buffer[0];
	}

	public void writeByte(byte value) throws IOException{
		buffer[0] = value;
		//this.randomAccessFile.write(this.buffer, 0, 1);
		out.write(buffer, 0, 1);
		innerPointer += 1;
	}
	
	public char readChar() throws IOException {
		
		int i = in.read(buffer, 0, 1);
		
		if(i > 0){
			innerPointer += i;
		}
		
		if(i != 1){
			throw new EOFException();
		}
		
		//this.randomAccessFile.readFully(buffer, 0, 1);
		return (char) buffer[0];
	}
	
	public void writeChar(char value) throws IOException{
		buffer[0] = (byte)value;
		//this.randomAccessFile.write(this.buffer, 0, 1);
		out.write(buffer, 0, 1);
		innerPointer += 1;
	}
	
	public String readString(int length) throws IOException{
		byte[] result = new byte[length];
		
		int i = in.read(result, 0, length);
		
		if(i > 0){
			innerPointer += i;
		}
		
		if(i != length){
			throw new EOFException();
		}
		
		//this.randomAccessFile.read(result, 0, length);
		return DataUtil.bytesToString(result, 0);
	}
	
	public void writeString(String value, int length) throws IOException{
		byte[] result = DataUtil.stringToBytes(value, length);
		//this.randomAccessFile.write(result, 0, length);
		out.write(result, 0, length);
		innerPointer += length;
	}
	
	public void seek(long pos) throws IOException{
		out.flush();
		in.clear();
		innerPointer = 0;
		randomAccessFile.seek(pos);
	}
	
	public int read(byte[] b) throws IOException{
		int i = in.read(b, 0, b.length);
		
		if(i > 0){
			innerPointer += i;
		}
		
		return i;
		//return this.randomAccessFile.read(b);
	}
	
	public int read(byte[] b, int off, int len) throws IOException{
		int i = in.read(b, off, len);
		
		if(i > 0){
			innerPointer += i;
		}

		return i;
		//return this.randomAccessFile.read(b, off, len);
	}
	
	public void write(byte[] b) throws IOException{
		//randomAccessFile.write(b);
		out.write(b);
		innerPointer += b.length;
	}
	
	public void write(byte[] b, int off, int len) throws IOException{
		//randomAccessFile.write(b, off, len);
		out.write(b, off, len);
		innerPointer += len;
	}
	
	public long getFilePointer() throws IOException{
		return randomAccessFile.getFilePointer() + innerPointer;
	}

	public long length() throws IOException{
		return randomAccessFile.length();
	}

	public void setLength(long value) throws IOException{
		in.clear();
		out.flush();
		innerPointer = 0;
		this.randomAccessFile.setLength(value);
	}
	public void close() throws IOException{
		out.flush();
		innerPointer = 0;
		randomAccessFile.close();
	}
	
	public void flush() throws IOException{
		this.out.flush();
		innerPointer = 0;
	}
	
	public void delete(){
		this.file.delete();
	}
	
	public RandomAccessFile getRandomAccessFile(){
		return randomAccessFile;
	}
	
	public File getFile(){
		return file;
	}
	
}
