package org.brandao.entityfilemanager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
		this.fileAccess = new FileAccess(this.file);
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
		
		this.fileAccess = new FileAccess(file);
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
	
	public void write(T ... value) throws IOException {
		write(value, 0, value.length, false);
	}

	public void writeRaw(R ... value) throws IOException {
		write(value, 0, value.length, false);
	}

	public void batchWrite(T[] values) throws IOException{
		write(values, 0, values.length, false);

	}
	public void batchWriteRaw(R[] values) throws IOException {
		write(values, 0, values.length, true);
	}
	
	public void write(T[] b, int off, int len) throws IOException{
		write(b, off, len, false);
	}
	
	public void writeRaw(R[] b, int off, int len) throws IOException {
		write(b, off, len, true);
	}
	
	@SuppressWarnings("unchecked")
	protected void write(Object[] b, int off, int len, boolean raw) throws IOException{
		
		long pointerOffset =
				dataHandler.getFirstPointer() +
				dataHandler.getFirstRecord() + 
				dataHandler.getRecordLength()*offset;
		int last       = off + len;
		long newOffset = this.offset + len;
		
		fileAccess.seek(pointerOffset);
		
		for(int i=off;i<last;i++){
			Object entity    = b[i];
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
		Object[] b = new Object[1];
		read(b, 0, 1, raw);
		return b[0];
	}
	
	protected Object[] batchRead(int len, boolean raw) throws IOException{
		
		long maxRead       = this.length - this.offset;
		int batch          = maxRead > len? len : (int)maxRead;

		Object[] result = 
				(Object[])Array.newInstance(
						raw? 
							this.dataHandler.getRawType() : 
							this.dataHandler.getType(), 
						batch
				);
		
		read(result, 0, batch, raw);
		return result;
	}

	public int read(T[] b, int off, int len) throws IOException {
		return read(b, off, len, false);
	}
	
	public int readRaw(R[] b, int off, int len) throws IOException {
		return read(b, off, len, true);
	}
	
	protected int read(Object[] b, int off, int len, boolean raw) throws IOException{
		
		long maxRead       = this.length - this.offset;
		int batch          = maxRead > len? len : (int)maxRead;
		int last           = off + batch;
		long pointerOffset = 
				this.dataHandler.getFirstPointer() +
				this.dataHandler.getFirstRecord() + 
				this.dataHandler.getRecordLength()*this.offset;
		
		this.fileAccess.seek(pointerOffset);
		
		for(int i=off;i<last;i++){
			b[i] = raw? this.dataHandler.readRaw(reader) : this.dataHandler.read(reader);
		}
		
		this.offset += batch;
		
		return batch;
	}
	
	public void reset() throws IOException{
		fileAccess.flush();
		fileAccess.setLength(dataHandler.getFirstPointer());
		fileAccess.seek(dataHandler.getFirstPointer());
		writeHeader();
		setLength(0);
		offset = 0;
		length = 0;
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
			write(array, 0, array.length, false);
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
