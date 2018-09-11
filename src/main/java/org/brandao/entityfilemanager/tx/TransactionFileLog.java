package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

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
	
	private	Checksum crc;

	private byte[] crcBuffer;
	
	public TransactionFileLog(FileAccess fa, TransactionReader reader,
			TransactionWritter writter, EntityFileTransactionManagerConfigurer eftmc) throws IOException{
		this.fa          = fa;
		this.reader      = reader;
		this.writter     = writter;
		this.eftmc       = eftmc;
		this.error       = null;
		this.lastPointer = 0;
		this.crc         = new CRC32();
		this.crcBuffer   = new byte[2048];
	}
	
	public File getFile(){
		return fa.getFile();
	}
	
	public void reset() throws IOException{
		fa.setLength(0);
		fa.seek(0);
		lastPointer = 0;
	}
	
	public void delete(){
		fa.delete();
	}

	public void close() throws IOException{
		fa.flush();
		fa.close();
	}

	public boolean isEmpty() throws IOException{
		return lastPointer == 0;
	}
	
	public void load() throws IOException{

		fa.seek(0);
		
		if(fa.length() == 0){
			return;
		}
		
		while(hasMoreElements()){
			nextElement();
		}
		
		fa.seek(0);
	}
	
	public Throwable getError(){
		return error;
	}
	
	public long getFilelength() throws IOException {
		return fa.length();
	}
	
	public void add(ConfigurableEntityFileTransaction ceft) throws IOException{
		
		//aponta para o último registro
		fa.seek(lastPointer);

		//marca o ínicio do registro com a posição do primeiro byte válido
		long start = fa.getFilePointer() + 8; //posição do primeiro byte válido
		fa.writeLong(start);
		
		//reserva espaço para armazenar o tamanho do registro
		fa.writeLong(-1);
		
		//registra a transação
		writter.write(ceft, fa);

		//atualiza o tamanho do registro
		long end = fa.getFilePointer() + 8;
		fa.seek(start);
		fa.writeLong(end);
		
		//calcula e armazena o crc no final do registro
		long crcValue = calculateCRC(start, end - 8);
		fa.seek(end - 8);
		fa.writeLong(crcValue);
		
		lastPointer = end;
	}
	
	public boolean hasMoreElements() {
		try{
			if(error != null || fa.getFilePointer() == fa.length()){
				return false;
			}
			
			long firstPointer = fa.readLong();
			
			if(firstPointer != fa.getFilePointer()){
				throw new IllegalStateException("start error: " + firstPointer + " <> " + fa.getFilePointer());
			}
			
			check();
			
			fa.seek(firstPointer);
			
			//return fa.getFilePointer() != fa.length();
			return true;
		}
		catch(Throwable e){
			error = e;
			return false;
		}
	}

	public void cutLog() throws IOException{
		fa.setLength(lastPointer);
	}
	
	public ConfigurableEntityFileTransaction nextElement() {
		
		ConfigurableEntityFileTransaction r = null;
		
		try{
			long nextPointer = fa.readLong();
			r = reader.read(eftmc, fa);
			fa.seek(nextPointer);
			lastPointer = nextPointer;
		}
		catch(TransactionException e){
			error = e;
		}
		catch(Throwable e){
			error = new IllegalStateException("invalid transaction file: " + fa.getFile().getName(), e);
		}
		
		return r;
	}

	private void check() throws IOException{
		
		long firstPointer = fa.getFilePointer();
		long lastPointer  = fa.readLong();

		if(lastPointer == -1){
			throw new IllegalStateException("invalid data: " + fa.getFilePointer());
		}
		
		if(fa.length() < lastPointer){
			throw new IllegalStateException("invalid size: " + fa.length() + " < " + lastPointer);
		}
		
		long crc = calculateCRC(firstPointer, lastPointer - 8);
		long pCrc = fa.readLong();
		
		if(crc != pCrc){
			throw new IllegalStateException("invalid crc: " + crc + " <> " + pCrc);
		}
		
	}
	
	private long calculateCRC(long firstPointer, long lastPointer) throws IOException{
		
		crc.reset();
		
		fa.seek(firstPointer);
		
		long counter = lastPointer - firstPointer;
		while(counter > 0){
			int maxRead = (int)(counter > crcBuffer.length? crcBuffer.length : counter);
			fa.read(crcBuffer, 0, maxRead);
			crc.update(crcBuffer, 0, maxRead);
			counter -= maxRead;
		}
		
		return crc.getValue();
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
