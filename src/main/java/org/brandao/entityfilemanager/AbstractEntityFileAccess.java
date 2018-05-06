package org.brandao.entityfilemanager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AbstractEntityFileAccess<T, R> 
	implements EntityFileAccess<T, R>{

	protected int recordLength;
	
	protected int firstRecord;
	
	protected long offset;
	
	protected File file;
	
	protected FileAccess fileAccess;

	protected ReadWriteLock lock;
	
	protected int batchLength;
	
	protected EntityFileDataHandler<T> dataHandler;
	
	public AbstractEntityFileAccess(File file, EntityFileDataHandler<T> dataHandler){
		this.file        = file;
		this.offset      = 0;
		this.lock        = new ReentrantReadWriteLock();
		this.batchLength = 1000;
		this.dataHandler = dataHandler;
	}
	
	public int getRecordLength() {
		return this.recordLength;
	}

	public int getFirstRecord() {
		return this.firstRecord;
	}
	
	public EntityFileDataHandler<T> getEntityFileDataHandler() {
		return this.dataHandler;
	}
	
	public long getOffset() throws IOException {
		return this.offset;
	}

	public String getName() {
		return this.file.getName();
	}
	
	public void setBatchLength(int value){
		this.batchLength = value;
	}

	public int getBatchLength(){
		return this.batchLength;
	}
	
	public File getAbsoluteFile() {
		return this.file.getAbsoluteFile();
	}

	public String getAbsolutePath() {
		return this.file.getAbsolutePath();
	}

	public void createNewFile() throws IOException {
		this.file.createNewFile();
		this.fileAccess = new FileAccess(this.file, new RandomAccessFile(this.file,"rw"));
		this.fileAccess.setLength(0);
		this.fileAccess.seek(0);
		this.writeHeader();
		this.fileAccess.seek(this.firstRecord);
	}
	
	protected void writeHeader() throws IOException{
		ByteArrayOutputStream stream = new ByteArrayOutputStream(this.firstRecord);
		DataOutputStream dStream = new DataOutputStream(stream);
		
		this.dataHandler.writeMetaData(dStream);
		
		if(stream.size() != this.firstRecord)
			throw new IOException(this.file.getName() + ": " + stream.size() + " <> " + this.firstRecord);
		
		byte[] data = stream.toByteArray();
		
		this.fileAccess.seek(0);
		this.fileAccess.write(data, 0, data.length);
	}

	public void open() throws IOException {
		
		if(!file.exists())
			throw new FileNotFoundException();
		
		this.fileAccess = new FileAccess(this.file, new RandomAccessFile(this.file,"rw"));
		this.readHeader();
		this.fileAccess.seek(this.firstRecord);
	}

	protected void readHeader() throws IOException{
		byte[] buffer = new byte[this.firstRecord];
		
		this.fileAccess.seek(0);
		this.fileAccess.read(buffer);
		
		ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
		DataInputStream dStream = new DataInputStream(stream);
		
		this.dataHandler.readMetaData(dStream);
	}

	public void seek(long value) throws IOException {
		long length = this.length();
		
		if(value > length)
			throw new IOException(this.file.getName() + ": entry not found: " + value);
		
		this.offset = value;
	}
	
	public void batchWrite(T[] entities) throws IOException{
		long pointerOffset = this.firstRecord;
		pointerOffset += this.recordLength*this.offset;
		
		int maxlength = 
				entities.length > this.batchLength?
						this.recordLength*this.batchLength :
						this.recordLength*entities.length;
		
		ByteArrayOutputStream stream = new ByteArrayOutputStream(maxlength);
		DataOutputStream dStream = new DataOutputStream(stream);
		
		for(T entity: entities){
			
			if(stream.size() + this.recordLength > maxlength){
				byte[] data = stream.toByteArray();
				this.fileAccess.seek(pointerOffset);
				this.fileAccess.write(data, 0, data.length);
				
				pointerOffset += data.length;
				stream.reset();
			}
			
			int startOffset = stream.size();
			this.dataHandler.write(dStream, entity);
			int endOffset = stream.size();
			int writeLength = endOffset - startOffset;
			
			if(writeLength != this.recordLength)
				throw new IOException(this.file.getName() + ": " + writeLength + " <> " + this.recordLength);
			
			this.offset++;
		}
		
		if(stream.size() != 0){

			if(pointerOffset + stream.size() >= this.fileAccess.length())
				this.dataHandler.writeEOF(dStream);
			
			byte[] data = stream.toByteArray();
			this.fileAccess.seek(pointerOffset);
			this.fileAccess.write(data, 0, data.length);
			
			pointerOffset += data.length;
		}
		else{
			
			if(pointerOffset + stream.size() >= this.fileAccess.length())
				this.dataHandler.writeEOF(dStream);
			
			byte[] data = stream.toByteArray();
			this.fileAccess.seek(pointerOffset);
			this.fileAccess.write(data, 0, data.length);
			
			pointerOffset += data.length;
		}
		
	}
	
	public void write(T entity) throws IOException {
		
		long pointerOffset  = this.firstRecord;
		pointerOffset      += this.recordLength*this.offset;
		
		ByteArrayOutputStream stream = new ByteArrayOutputStream(this.recordLength);
		DataOutputStream dStream = new DataOutputStream(stream);
		
		this.dataHandler.write(dStream, entity);
		
		if(stream.size() != this.recordLength)
			throw new IOException(this.file.getName() + ": " + stream.size() + " <> " + this.recordLength);
		
		
		if(pointerOffset + this.recordLength == this.fileAccess.length())
			this.dataHandler.writeEOF(dStream);
		
		byte[] data = stream.toByteArray();
		this.fileAccess.seek(pointerOffset);
		this.fileAccess.write(data, 0, data.length);
		
		this.offset++;
	}

	public void batchWriteRawEntity(R[] entities) throws IOException {
	}
	
	public void writeRawEntity(R value) throws IOException {
	}

	public T[] batchRead(int len) throws IOException {
		return null;
	}
	
	public T read() throws IOException {
		long pointerOffset = this.firstRecord;
		pointerOffset += this.recordLength*this.offset;
		
		byte[] buffer = new byte[this.recordLength];
		
		this.fileAccess.seek(pointerOffset);
		this.fileAccess.read(buffer);
		
		ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
		DataInputStream dStream = new DataInputStream(stream);
		
		T entity = this.dataHandler.read(dStream);
		
		this.offset++;
		
		return entity;
	}

	public R[] batchReadRawEntity(int len) throws IOException {
		return null;
	}
	
	public R readRawEntity() throws IOException {
		return null;
	}
	
	public long length() throws IOException {
		return 
			(this.fileAccess.length() - this.firstRecord) / this.recordLength;
	}
	
	public void setLength(long value) throws IOException {
		
		long filePointer = this.firstRecord + value*this.recordLength;
		
		ByteArrayOutputStream stream = new ByteArrayOutputStream(this.recordLength);
		DataOutputStream dStream = new DataOutputStream(stream);
		
		this.dataHandler.writeEOF(dStream);
		
		byte[] data = stream.toByteArray();
		
		this.fileAccess.setLength(filePointer);
		this.fileAccess.seek(filePointer);
		this.fileAccess.write(data, 0, data.length);
	}
	
	public void flush() throws IOException {
	}

	public void close() throws IOException {
		if(this.fileAccess != null)
			this.fileAccess.close();
	}

	public boolean exists() {
		return this.file.exists();
	}
	
	public ReadWriteLock getLock(){
		return this.lock;
	}

	public Class<T> getType() {
		// TODO Auto-generated method stub
		return null;
	}

}
