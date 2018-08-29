package org.brandao.entityfilemanager;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileAccess {

	private static final byte WRITE = 0;
	
	private static final byte READ = 1;
	
	private byte[] buffer = new byte[8];
	
	private RandomAccessFile randomAccessFile;
	
	private File file;
	
	private BufferedWritterRandomAccessFile writter;
	
	private BufferedReaderRandomAccessFile reader;
	
	private byte lastOP;
	
	private long pointer;
	
	public FileAccess(File file, RandomAccessFile randomAccessFile) throws IOException{
		this(file, randomAccessFile, 2048, 2048);
	}
	
	public FileAccess(File file, RandomAccessFile randomAccessFile, 
			int readCapacity, int writeCapacity) throws IOException{
		this.randomAccessFile = randomAccessFile;
		this.file = file;
		this.lastOP = 0;
		this.reader = new BufferedReaderRandomAccessFile(readCapacity, randomAccessFile);
		this.writter = new BufferedWritterRandomAccessFile(writeCapacity, randomAccessFile);
		this.pointer = randomAccessFile.getFilePointer();
	}

	public long readLong() throws IOException {
		readFully(buffer, 0, 8);
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
		writebytes(buffer, 0, 8);
		//this.randomAccessFile.write(this.buffer, 0, 8);
	}
	
	public int readInt() throws IOException {
		readFully(buffer, 0, 4);
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
		writebytes(buffer, 0, 4);
	}

	public short readShort() throws IOException {
		readFully(buffer, 0, 2);
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
		writebytes(buffer, 0, 2);
	}

	public byte readByte() throws IOException {
		//this.randomAccessFile.readFully(buffer, 0, 1);
		readFully(buffer, 0, 1);
		return buffer[0];
	}

	public void writeByte(byte value) throws IOException{
		buffer[0] = value;
		//this.randomAccessFile.write(this.buffer, 0, 1);
		writebytes(buffer, 0, 1);
	}
	
	public char readChar() throws IOException {
		//this.randomAccessFile.readFully(buffer, 0, 1);
		readFully(buffer, 0, 1);
		return (char) buffer[0];
	}
	
	public void writeChar(char value) throws IOException{
		buffer[0] = (byte)value;
		//this.randomAccessFile.write(this.buffer, 0, 1);
		writebytes(buffer, 0, 1);
	}
	
	public String readString(int length) throws IOException{
		byte[] result = new byte[length];
		//this.randomAccessFile.read(result, 0, length);
		readFully(result, 0, length);
		return DataUtil.bytesToString(result, 0);
	}
	
	public void writeString(String value, int length) throws IOException{
		byte[] result = DataUtil.stringToBytes(value, length);
		//this.randomAccessFile.write(result, 0, length);
		writebytes(result, 0, length);
	}

	public String[] readStrings(int length) throws IOException{
		byte[] result = new byte[length];
		//this.randomAccessFile.read(result, 0, length);
		readFully(result, 0, length);
		return DataUtil.bytesToStrings(result, 0);
	}
	
	public void writeString(String[] value, int length) throws IOException{
		byte[] buffer = DataUtil.stringsToBytes(value, length);
		write(buffer, 0, length);
	}
	
	public void seek(long pos) throws IOException{
		writter.flush();
		reader.clear();
		pointer = pos;
		randomAccessFile.seek(pos);
	}
	
	public int read(byte[] b) throws IOException{
		return readbytes(b);
		//return this.randomAccessFile.read(b);
	}
	
	public int read(byte[] b, int off, int len) throws IOException{
		return readbytes(b, off, len);
		//return this.randomAccessFile.read(b, off, len);
	}
	
	public void write(byte[] b) throws IOException{
		//this.randomAccessFile.write(b);
		writebytes(b, 0, b.length);
	}
	
	public void write(byte[] b, int off, int len) throws IOException{
		//this.randomAccessFile.write(b, off, len);
		writebytes(b, off, len);
	}
	
	public long getFilePointer() throws IOException{
		return pointer;
	}

	public long length() throws IOException{
		return this.randomAccessFile.length();
	}

	public void setLength(long value) throws IOException{
		reader.clear();
		writter.flush();
		this.randomAccessFile.setLength(value);
	}
	
	public void close() throws IOException{
		this.randomAccessFile.close();
	}
	
	public void delete(){
		this.file.delete();
	}
	
	public RandomAccessFile getRandomAccessFile(){
		return this.randomAccessFile;
	}
	
	public File getFile(){
		return this.file;
	}
	
	public void flush() throws IOException{
		writter.flush();
		reader.clear();
	}
	private void resyncBuffer(byte type) throws IOException{
		
		if(type != lastOP){
			if(lastOP == WRITE){
				writter.flush();
				reader.clear();
			}
			
			randomAccessFile.seek(pointer);
		}
		
		lastOP = type;
	}
	
	private int readbytes(byte[] b) throws IOException{
		resyncBuffer(READ);
		int i = reader.read(b, 0, b.length);

		if(i > 0){
			pointer += i;
		}
		
		return i;
	}
	
	private int readbytes(byte[] b, int off, int len) throws IOException{
		resyncBuffer(READ);
		int i = reader.read(b, off, len);

		if(i > 0){
			pointer += i;
		}
		
		return i;
	}

	private int readFully(byte[] b, int off, int len) throws IOException{
		resyncBuffer(READ);
		int i = reader.read(b, off, len);
		
		if(i > 0){
			pointer += i;
		}
		
		if(i != len){
			throw new EOFException();
		}
		
		return i;
	}

	private void writebytes(byte[] b, int off, int len) throws IOException{
		resyncBuffer(WRITE);
		writter.write(b, off, len);
		pointer += len;
	}
	
}
