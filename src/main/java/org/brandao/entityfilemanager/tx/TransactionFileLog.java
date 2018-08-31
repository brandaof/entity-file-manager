package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.util.Enumeration;

import org.brandao.entityfilemanager.DataReader;
import org.brandao.entityfilemanager.DataWritter;
import org.brandao.entityfilemanager.FileAccess;

public class TransactionFileLog
	implements Enumeration<ConfigurableEntityFileTransaction>, DataReader, DataWritter{

	private FileAccess fa;
	
	private TransactionReader reader;

	private TransactionWritter writter;
	
	private EntityFileTransactionManagerConfigurer eftmc;
	
	private long lastPointer;
	
	private Throwable error;
	
	private long length;
	
	public TransactionFileLog(FileAccess fa, TransactionReader reader,
			TransactionWritter writter, EntityFileTransactionManagerConfigurer eftmc) throws IOException{
		this.fa          = fa;
		this.reader      = reader;
		this.writter     = writter;
		this.eftmc       = eftmc;
		this.error       = null;
		this.lastPointer = 0;
		this.length      = 0;
	}
	
	public void reset() throws IOException{
		fa.setLength(0);
		fa.seek(0);
	}
	
	public void delete(){
		fa.delete();
	}

	public void close() throws IOException{
		fa.flush();
		fa.close();
	}

	public void load() throws IOException{

		length = 0;
		
		if(fa.length() == 0){
			fa.seek(0);
			return;
		}
		
		fa.seek(0);
		
		while(hasMoreElements()){
			length++;
		}
		
		lastPointer = fa.getFilePointer();
	}
	
	public Throwable getError(){
		return error;
	}
	
	public long length() {
		return length;
	}
	
	public void add(ConfigurableEntityFileTransaction ceft) throws IOException{
		
		fa.seek(lastPointer);

		fa.writeLong(fa.getFilePointer() + 8);
		writter.write(ceft, fa);
		
		lastPointer = fa.getFilePointer();
		length++;
	}
	
	public boolean hasMoreElements() {
		try{
			if(length == 0){
				return false;
			}
			
			long pointer = fa.readLong();
			return 
				pointer == fa.getFilePointer() &&
				fa.getFilePointer() != fa.length();
		}
		catch(Throwable e){
			error = e;
			return false;
		}
	}

	public ConfigurableEntityFileTransaction nextElement() {
		
		ConfigurableEntityFileTransaction r = null;
		
		try{
			r = reader.read(eftmc, fa);
		}
		catch(TransactionException e){
			error = e;
		}
		catch(Throwable e){
			error = new IllegalStateException("invalid transaction file: " + fa.getFile().getName(), e);
		}
		
		return r;
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

	public String[] readStrings(int length) throws IOException {
		return fa.readStrings(length);
	}
}
