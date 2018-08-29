package org.brandao.entityfilemanager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AbstractEntityFileAccess<T, R, H> 
	implements EntityFileAccess<T, R, H>{

	protected String name;
	
	protected H metadata;
	
	protected long offset;
	
	protected File file;
	
	protected FileAccess fileAccess;

	protected Lock lock;
	
	protected int batchLength;
	
	protected EntityFileDataHandler<T, R, H> dataHandler;
	
	protected long length;
	
	protected DataWritter writter;

	protected DataReader reader;
	
	public AbstractEntityFileAccess(String name, File file, EntityFileDataHandler<T, R, H> dataHandler){
		this.name         = name;
		this.file         = file;
		this.offset       = 0;
		this.batchLength  = 1000;
		this.dataHandler  = dataHandler;
		this.lock         = new ReentrantLock();
	}
	
	public EntityFileDataHandler<T, R, H> getEntityFileDataHandler() {
		return this.dataHandler;
	}
	
	public long getOffset() throws IOException {
		return this.offset;
	}

	public String getName() {
		return this.name;
	}
	
	public void setBatchLength(int value){
		this.batchLength = value;
	}

	public int getBatchLength(){
		return batchLength;
	}
	
	public File getAbsoluteFile() {
		return file == null? null : file.getAbsoluteFile();
	}

	public String getAbsolutePath() {
		return file == null? null : file.getAbsolutePath();
	}

	public void createNewFile() throws IOException {
		this.file.createNewFile();
		this.fileAccess = new FileAccess(this.file, new RandomAccessFile(this.file, "rw"));
		this.writter    = new FileAccessDataWritter(fileAccess);
		this.reader     = new FileAccessDataReader(fileAccess);
		this.fileAccess.setLength(0);
		this.fileAccess.seek(0);
		this.writeHeader();
		this.setLength(0);
	}
	
	protected void writeHeader() throws IOException{
		
		long len = fileAccess.getFilePointer();
		dataHandler.writeMetaData(writter, metadata);
		len = fileAccess.getFilePointer() - len;
		
		if(len != dataHandler.getFirstRecord()){
			throw new IOException(this.file.getName() + ": " + len + " <> " + dataHandler.getFirstRecord());
		}
		
	}
	
	public void open() throws IOException {
		
		if(!file.exists())
			throw new FileNotFoundException();
		
		this.fileAccess = new FileAccess(file, new RandomAccessFile(file, "rw"));
		this.writter    = new FileAccessDataWritter(fileAccess);
		this.readHeader();
		this.fileAccess.seek(dataHandler.getFirstPointer() + dataHandler.getFirstRecord());
		this.length = 
				(
					fileAccess.length() - 
					dataHandler.getFirstPointer() -
					dataHandler.getFirstRecord() - 
					dataHandler.getEOFLength()
				) /
					dataHandler.getRecordLength(); 

	}

	protected void readHeader() throws IOException{
		fileAccess.seek(dataHandler.getFirstPointer());
		metadata = dataHandler.readMetaData(reader);
	}

	public void seek(long value) throws IOException {
		
		if(value > this.length)
			throw new IOException(file.getName() + ": entry not found: " + value);
		
		this.offset = value;
	}
	
	public void batchWrite(T[] values) throws IOException{
		this.batchWrite(values, false);
	}
	
	public void write(T value) throws IOException {
		this.write(value, false);
	}

	public void batchWriteRaw(R[] values) throws IOException {
		this.batchWrite(values, true);
	}
	
	public void writeRaw(R value) throws IOException {
		this.write(value, true);
	}

	@SuppressWarnings("unchecked")
	protected void write(Object entity, boolean raw) throws IOException {
		
		long newOffset = offset + 1;
		
		long pointerOffset = 
				dataHandler.getFirstPointer() +
				dataHandler.getFirstRecord() + 
				dataHandler.getRecordLength()*offset;
		
		fileAccess.seek(pointerOffset);
		
		long len = fileAccess.getFilePointer();
		if(raw){
			dataHandler.writeRaw(writter, (R)entity);
		}
		else{
			dataHandler.write(writter, (T)entity);
		}
		len = fileAccess.getFilePointer() - len;
		
		if(len != dataHandler.getRecordLength()){
			throw new IOException(file.getName() + ": " + len + " <> " + dataHandler.getRecordLength());
		}
		
		if(newOffset >= length){
			this.dataHandler.writeEOF(writter);
		}
		
		offset = newOffset;
		
		if(newOffset >= length){
			length++;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	protected void batchWrite(Object[] entities, boolean raw) throws IOException{
		long pointerOffset =
				dataHandler.getFirstPointer() +
				dataHandler.getFirstRecord() + 
				dataHandler.getRecordLength()*offset;
		
		long newOffset = this.offset + entities.length;
		
		fileAccess.seek(pointerOffset);
		
		for(Object entity: entities){
			
			long startOffset = fileAccess.getFilePointer();

			if(raw){
				dataHandler.writeRaw(writter, (R)entity);
			}
			else{
				dataHandler.write(writter, (T)entity);
			}
			
			long endOffset   = fileAccess.getFilePointer();
			long writeLength = endOffset - startOffset;
			
			if(writeLength != dataHandler.getRecordLength())
				throw new IOException(file.getName() + ": " + writeLength + " <> " + dataHandler.getRecordLength());
			
		}
		
		if(newOffset >= length){
			dataHandler.writeEOF(writter);
		}
		
		offset = newOffset;
		
		if(newOffset >= length){
			length = newOffset;
		}
	}
	
	@SuppressWarnings("unchecked")
	public T[] batchRead(int len) throws IOException {
		return (T[]) this.batchRead(len, false);
	}
	
	@SuppressWarnings("unchecked")
	public T read() throws IOException {
		return (T) this.read(false);
	}

	@SuppressWarnings("unchecked")
	public R[] batchReadRaw(int len) throws IOException {
		return (R[])this.batchRead(len, true);
	}
	
	@SuppressWarnings("unchecked")
	public R readRaw() throws IOException {
		return (R) this.read(true);
	}
	
	protected Object read(boolean raw) throws IOException {
		
		long pointerOffset = 
				this.dataHandler.getFirstPointer() +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*this.offset;
		
		this.fileAccess.seek(pointerOffset);
		Object entity = raw? this.dataHandler.readRaw(reader) : this.dataHandler.read(reader);
		
		this.offset++;
		
		return entity;
	}
	
	protected Object[] batchRead(int len, boolean raw) throws IOException{
		
		long maxRead       = this.length - this.offset;
		int batch          = maxRead > len? len : (int)maxRead;
		long pointerOffset = 
				this.dataHandler.getFirstPointer() +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*this.offset;
		
		this.fileAccess.seek(pointerOffset);
				
		Object[] result = 
				(Object[])Array.newInstance(
						raw? 
							this.dataHandler.getRawType() : 
							this.dataHandler.getType(), 
						batch
				);
		
		for(int i=0;i<batch;i++){
			result[i] = raw? this.dataHandler.readRaw(reader) : this.dataHandler.read(reader);
		}
		
		this.offset += batch;
		
		return result;
	}
	
	public long length() throws IOException {
		return this.length;
	}
	
	@SuppressWarnings("unchecked")
	public void setLength(long value) throws IOException {
		
		
		if(value > length){
			int op = (int)(value - length);
			offset = length;
			T[] array = (T[])Array.newInstance(dataHandler.getType(), op);
			batchWrite(array, false);
		}
		else{
			long fileLength = 
					dataHandler.getFirstPointer() +
					dataHandler.getHeaderLength() + 
					dataHandler.getRecordLength()*value + 
					dataHandler.getEOFLength();
			
			
			fileAccess.setLength(fileLength);
			fileAccess.seek(fileLength - dataHandler.getEOFLength());
			dataHandler.writeEOF(writter);
		}
		
		length = value;
		
	}
	
	public void flush() throws IOException {
		this.fileAccess.flush();
	}

	public void close() throws IOException {
		if(this.fileAccess != null)
			this.fileAccess.close();
	}

	public boolean exists() {
		return file == null? false : file.exists();
	}
	
	public Lock getLock(){
		return this.lock;
	}

	public Class<T> getType() {
		return this.dataHandler.getType();
	}

	public Class<R> getRawType() {
		return this.dataHandler.getRawType();
	}

	public H getMetadata() {
		return this.metadata;
	}

	public void delete() throws IOException {
		this.file.delete();
	}
    
}
