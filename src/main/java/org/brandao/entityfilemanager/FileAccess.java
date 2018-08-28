package org.brandao.entityfilemanager;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileAccess {

	private static final byte WRITE = 0;
	
	private static final byte READ = 1;
	
	private byte[] buffer = new byte[8];
	
	private RandomAccessFile randomAccessFile;
	
	private File file;
	
	private BufferedOutputStream out;
	
	private BufferedInputStream in;
	
	private byte lastOP;
	
	private long pointer;
	
	private long readCount;

	private long writeCount;
	
	public FileAccess(File file, RandomAccessFile randomAccessFile) throws IOException{
		this(file, randomAccessFile, 8192, 8192);
	}
	
	public FileAccess(File file, RandomAccessFile randomAccessFile, 
			int readCapacity, int writeCapacity) throws IOException{
		this.randomAccessFile = randomAccessFile;
		this.file = file;
		this.lastOP = 0;
		this.readCount = 0;
		this.writeCount = 0;
		this.in = new BufferedInputStream(readCapacity, new FileInputStream(randomAccessFile.getFD()));
		this.out = new BufferedOutputStream(writeCapacity, new FileOutputStream(randomAccessFile.getFD()));
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
	
	public void seek(long pos) throws IOException{
		this.randomAccessFile.seek(pos);
		out.flush();
		in.clear();
		readCount = 0;
		writeCount = 0;
		pointer = pos;
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
		writebytes(b);
	}
	
	public void write(byte[] b, int off, int len) throws IOException{
		//this.randomAccessFile.write(b, off, len);
		writebytes(b, off, len);
	}
	
	public long getFilePointer() throws IOException{
		return lastOP == READ? 
				pointer + readCount : 
				(lastOP == WRITE? pointer + writeCount : this.randomAccessFile.getFilePointer());
	}

	public long length() throws IOException{
		return this.randomAccessFile.length();
	}

	public void setLength(long value) throws IOException{
		in.clear();
		out.flush();
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
		out.flush();
		in.clear();
	}
	private void resyncBuffer(byte type) throws IOException{
		
		if(type != lastOP){
			if(lastOP == WRITE){
				out.flush();
				in.clear();
				pointer   += writeCount;
				writeCount = 0;
			}
			else{
				pointer  += readCount;
				readCount = 0;
			}
			
			randomAccessFile.seek(pointer);
		}
		
		lastOP = type;
	}
	
	private int readbytes(byte[] b) throws IOException{
		resyncBuffer(READ);
		int i = in.read(b);

		if(i > 0){
			readCount += i;
		}
		
		return i;
	}
	
	private int readbytes(byte[] b, int off, int len) throws IOException{
		resyncBuffer(READ);
		int i = in.read(b, off, len);

		if(i > 0){
			readCount += i;
		}
		
		return i;
	}

	private int readFully(byte[] b) throws IOException{
		resyncBuffer(READ);
		int i = in.read(b, 0, b.length);
		
		if(i > 0){
			readCount += i;
		}
		
		if(i != b.length){
			throw new EOFException();
		}
		
		return i;
	}
	
	private int readFully(byte[] b, int off, int len) throws IOException{
		resyncBuffer(READ);
		int i = in.read(b, off, len);
		
		if(i > 0){
			readCount += i;
		}
		
		if(i != len){
			throw new EOFException();
		}
		
		return i;
	}

	private void writebytes(byte[] b) throws IOException{
		resyncBuffer(WRITE);
		out.write(b);
		writeCount += b.length;
	}
	
	private void writebytes(byte[] b, int off, int len) throws IOException{
		resyncBuffer(WRITE);
		out.write(b, off, len);
		writeCount += len;
	}
	
}
