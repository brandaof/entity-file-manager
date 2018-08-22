package org.brandao.entityfilemanager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AbstractEntityFileAccess<T, R> 
	implements EntityFileAccess<T, R>{

	protected int recordLength;
	
	protected int eofLength;
	
	protected int firstRecord;

	protected long length;
	
	protected long offset;
	
	protected File file;
	
	protected FileAccess fileAccess;

	protected ReadWriteLock lock;
	
	protected int batchLength;
	
	protected EntityFileDataHandler<T, R> dataHandler;
	
	public AbstractEntityFileAccess(File file, EntityFileDataHandler<T, R> dataHandler){
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
	
	public EntityFileDataHandler<T, R> getEntityFileDataHandler() {
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
		this.fileAccess = new FileAccess(this.file, new RandomAccessFile(this.file, "rw"));
		this.fileAccess.setLength(0);
		this.fileAccess.seek(0);
		this.writeHeader();
		this.fileAccess.seek(this.firstRecord);
		this.length = 0;
	}
	
	protected void writeHeader() throws IOException{
		ByteArrayOutputStream stream = new ByteArrayOutputStream(this.firstRecord);
		DataOutputStream dStream     = new DataOutputStream(stream);
		
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
		
		this.fileAccess = new FileAccess(this.file, new RandomAccessFile(this.file, "rw"));
		this.readHeader();
		this.fileAccess.seek(this.firstRecord);
		this.length = (this.fileAccess.length() - this.firstRecord) / this.recordLength;
	}

	protected void readHeader() throws IOException{
		byte[] buffer = new byte[this.firstRecord];
		
		this.fileAccess.seek(0);
		this.fileAccess.read(buffer);
		
		ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
		DataInputStream dStream     = new DataInputStream(stream);
		
		this.dataHandler.readMetaData(dStream);
	}

	public void seek(long value) throws IOException {
		
		if(value > this.length)
			throw new IOException(this.file.getName() + ": entry not found: " + value);
		
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
	private void write(Object entity, boolean raw) throws IOException {
		
		ByteArrayOutputStream stream = new ByteArrayOutputStream(this.recordLength + this.eofLength);
		DataOutputStream dStream     = new DataOutputStream(stream);
		long eof                     = this.offset + 1 - this.length;
		
		if(raw){
			this.dataHandler.writeRaw(dStream, (R)entity);
		}
		else{
			this.dataHandler.write(dStream, (T)entity);
		}
		
		if(stream.size() != this.recordLength){
			throw new IOException(this.file.getName() + ": " + stream.size() + " <> " + this.recordLength);
		}
		
		
		if(eof == 0){
			this.dataHandler.writeEOF(dStream);
		}
		
		long pointerOffset = this.firstRecord + this.recordLength*this.offset;
		byte[] data        = stream.toByteArray();
		
		this.fileAccess.seek(pointerOffset);
		this.fileAccess.write(data, 0, data.length);
		
		this.offset++;
		
		if(eof == 0){
			this.length++;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void batchWrite(Object[] entities, boolean raw) throws IOException{
		
		long pointerOffset = this.firstRecord + this.recordLength*this.offset;
		
		int maxlength = 
				entities.length > this.batchLength?
						this.recordLength*this.batchLength :
						this.recordLength*entities.length;
		
		ByteArrayOutputStream stream = new ByteArrayOutputStream(maxlength + this.eofLength);
		DataOutputStream dStream     = new DataOutputStream(stream);
		long eof                     = this.offset + entities.length - this.length;
		
		for(Object entity: entities){
			
			if(stream.size() + this.recordLength > maxlength){
				byte[] data = stream.toByteArray();
				this.fileAccess.seek(pointerOffset);
				this.fileAccess.write(data, 0, data.length);
				
				pointerOffset += data.length;
				stream.reset();
			}
			
			int startOffset = stream.size();

			if(raw){
				this.dataHandler.writeRaw(dStream, (R)entity);
			}
			else{
				this.dataHandler.write(dStream, (T)entity);
			}
			
			int endOffset   = stream.size();
			int writeLength = endOffset - startOffset;
			
			if(writeLength != this.recordLength)
				throw new IOException(this.file.getName() + ": " + writeLength + " <> " + this.recordLength);
			
		}
		
		if(eof >= 0){
			this.dataHandler.writeEOF(dStream);
		}
		
		byte[] data = stream.toByteArray();
		this.fileAccess.seek(pointerOffset);
		this.fileAccess.write(data, 0, data.length);
		
		this.offset += entities.length;
		
		if(eof >= 0){
			this.length += 1 + eof;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public T[] batchRead(int len) throws IOException {
		return (T[]) this.batchRead(false);
	}
	
	@SuppressWarnings("unchecked")
	public T read() throws IOException {
		return (T) this.read(false);
	}

	@SuppressWarnings("unchecked")
	public R[] batchReadRaw(int len) throws IOException {
		return (R[])this.batchRead(true);
	}
	
	@SuppressWarnings("unchecked")
	public R readRaw() throws IOException {
		return (R) this.read(false);
	}
	
	private Object read(boolean raw) throws IOException {
		
		long pointerOffset = this.firstRecord + this.recordLength*this.offset;
		
		byte[] buffer = new byte[this.recordLength];
		
		this.fileAccess.seek(pointerOffset);
		this.fileAccess.read(buffer);
		
		ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
		DataInputStream dStream     = new DataInputStream(stream);
		
		Object entity = raw? this.dataHandler.readRaw(dStream) : this.dataHandler.read(dStream);
		
		this.offset++;
		
		return entity;
	}
	
	private Object[] batchRead(boolean raw) throws IOException{
		
		long maxRead       = this.length - this.offset;
		int batch          = maxRead > this.batchLength? this.batchLength : (int)maxRead;
		int maxlength      = this.recordLength*batch;
		long pointerOffset = this.firstRecord + this.recordLength*this.offset;
		
		byte[] buffer = new byte[maxlength];
				
		this.fileAccess.seek(pointerOffset);
		this.fileAccess.read(buffer);
				
		ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
		DataInputStream dStream     = new DataInputStream(stream);

		Object[] result = 
				(Object[])Array.newInstance(
						raw? 
							this.dataHandler.getRawType() : 
							this.dataHandler.getType(), 
						batch
				);
		
		for(int i=0;i<batch;i++){
			result[i] = raw? this.dataHandler.readRaw(dStream) : this.dataHandler.read(dStream);
		}
		
		this.offset += batch;
		
		return result;
	}
	
	public long length() throws IOException {
		return this.length;
		//return 
		//	(this.fileAccess.length() - this.firstRecord) / this.recordLength;
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
		return this.dataHandler.getType();
	}

	public Class<R> getRawType() {
		return this.dataHandler.getRawType();
	}

}
