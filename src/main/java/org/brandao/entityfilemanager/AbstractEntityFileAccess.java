package org.brandao.entityfilemanager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	
	protected long firstPointer;
	
	public AbstractEntityFileAccess(String name, File file, EntityFileDataHandler<T, R, H> dataHandler){
		this.name         = name;
		this.file         = file;
		this.offset       = 0;
		this.batchLength  = 1000;
		this.dataHandler  = dataHandler;
		this.lock         = new ReentrantLock();
		this.firstPointer = 0;
		
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
		this.fileAccess.setLength(0);
		this.fileAccess.seek(0);
		this.writeHeader();
		this.setLength(0);
	}
	
	protected void writeHeader() throws IOException{
		ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getHeaderLength());
		DataOutputStream dStream     = new DataOutputStream(stream);
		
		this.dataHandler.writeMetaData(dStream, this.metadata);
		
		if(stream.size() != this.dataHandler.getFirstRecord())
			throw new IOException(this.file.getName() + ": " + stream.size() + " <> " + this.dataHandler.getFirstRecord());
		
		byte[] data = stream.toByteArray();
		
		this.fileAccess.seek(this.firstPointer);
		this.fileAccess.write(data, 0, data.length);
	}

	public void open() throws IOException {
		
		if(!file.exists())
			throw new FileNotFoundException();
		
		this.fileAccess = new FileAccess(this.file, new RandomAccessFile(this.file, "rw"));
		this.readHeader();
		this.fileAccess.seek(this.dataHandler.getFirstRecord());
		this.length = 
				(
					this.fileAccess.length() - 
					this.dataHandler.getFirstRecord() - 
					this.dataHandler.getEOFLength()
				) /
					this.dataHandler.getRecordLength(); 

	}

	protected void readHeader() throws IOException{
		byte[] buffer = new byte[this.dataHandler.getHeaderLength()];
		
		this.fileAccess.seek(this.firstPointer);
		this.fileAccess.read(buffer);
		
		ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
		DataInputStream dStream     = new DataInputStream(stream);
		
		this.metadata = this.dataHandler.readMetaData(dStream);
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
	protected void write(Object entity, boolean raw) throws IOException {
		
		ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getRecordLength() + this.dataHandler.getEOFLength());
		DataOutputStream dStream     = new DataOutputStream(stream);
		long newOffset               = this.offset + 1;
		
		if(raw){
			this.dataHandler.writeRaw(dStream, (R)entity);
		}
		else{
			this.dataHandler.write(dStream, (T)entity);
		}
		
		if(stream.size() != this.dataHandler.getRecordLength()){
			throw new IOException(this.file.getName() + ": " + stream.size() + " <> " + this.dataHandler.getRecordLength());
		}
		
		
		if(newOffset >= this.length){
			this.dataHandler.writeEOF(dStream);
		}
		
		long pointerOffset = 
				this.firstPointer +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*this.offset;
		
		byte[] data        = stream.toByteArray();

		this.fileAccess.seek(pointerOffset);
		this.fileAccess.write(data, 0, data.length);
		
		this.offset = newOffset;
		
		if(newOffset >= this.length){
			this.length++;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	protected void batchWrite(Object[] entities, boolean raw) throws IOException{
		
		long pointerOffset =
				this.firstPointer +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*this.offset;
		
		int maxlength = 
				entities.length > this.batchLength?
						this.dataHandler.getRecordLength()*this.batchLength :
						this.dataHandler.getRecordLength()*entities.length;
		
		ByteArrayOutputStream stream = new ByteArrayOutputStream(maxlength + this.dataHandler.getEOFLength());
		DataOutputStream dStream     = new DataOutputStream(stream);
		long newOffset               = this.offset + entities.length;
		
		for(Object entity: entities){
			
			if(stream.size() + this.dataHandler.getRecordLength() > maxlength){
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
			
			if(writeLength != this.dataHandler.getRecordLength())
				throw new IOException(this.file.getName() + ": " + writeLength + " <> " + this.dataHandler.getRecordLength());
			
		}
		
		if(newOffset >= this.length){
			this.dataHandler.writeEOF(dStream);
		}
		
		byte[] data = stream.toByteArray();
		this.fileAccess.seek(pointerOffset);
		this.fileAccess.write(data, 0, data.length);
		
		this.offset = newOffset;
		
		if(newOffset >= this.length){
			this.length = newOffset;
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
				this.firstPointer +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*this.offset;
		
		byte[] buffer = new byte[this.dataHandler.getRecordLength()];
		
		this.fileAccess.seek(pointerOffset);
		this.fileAccess.read(buffer);
		
		ByteArrayInputStream stream = new ByteArrayInputStream(buffer);
		DataInputStream dStream     = new DataInputStream(stream);
		
		Object entity = raw? this.dataHandler.readRaw(dStream) : this.dataHandler.read(dStream);
		
		this.offset++;
		
		return entity;
	}
	
	protected Object[] batchRead(int len, boolean raw) throws IOException{
		
		long maxRead       = this.length - this.offset;
		int batch          = maxRead > len? len : (int)maxRead;
		int maxlength      = this.dataHandler.getRecordLength()*batch;
		long pointerOffset = 
				this.firstPointer +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*this.offset;
		
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
	}
	
	@SuppressWarnings("unchecked")
	public void setLength(long value) throws IOException {
		
		
		if(value > this.length){
			int op = (int)(value - this.length);
			this.offset = this.length;
			T[] array = (T[])Array.newInstance(this.dataHandler.getType(), op);
			this.batchWrite(array, false);
		}
		else{
			long fileLength = 
					this.firstPointer +
					this.dataHandler.getFirstRecord() + 
					this.dataHandler.getRecordLength()*value + 
					this.dataHandler.getEOFLength();
			
			ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getEOFLength());
			DataOutputStream dStream     = new DataOutputStream(stream);
			
			this.dataHandler.writeEOF(dStream);
			
			this.fileAccess.setLength(fileLength);
			this.fileAccess.seek(this.fileAccess.length() - this.dataHandler.getEOFLength());
			this.fileAccess.write(stream.toByteArray());
			
		}
		
		this.length = value;
		
	}
	
	public void flush() throws IOException {
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

	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeObject(metadata);
		stream.writeUTF(file.getName());
		stream.writeInt(batchLength);
		stream.writeObject(dataHandler);
    }

    @SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    	metadata    = (H) stream.readObject();
		file        = new File(stream.readUTF());
		batchLength = stream.readInt();
		dataHandler = (EntityFileDataHandler<T, R, H>) stream.readObject();
		this.lock   = new ReentrantLock();
		
		this.open();
    }
    
}
