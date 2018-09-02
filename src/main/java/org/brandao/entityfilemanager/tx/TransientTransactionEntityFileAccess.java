package org.brandao.entityfilemanager.tx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.brandao.entityfilemanager.DataReader;
import org.brandao.entityfilemanager.DataReaderInputStream;
import org.brandao.entityfilemanager.DataWritter;
import org.brandao.entityfilemanager.DataWritterOutputStream;
import org.brandao.entityfilemanager.EntityFileAccess;

public class TransientTransactionEntityFileAccess<T, R, H> 
	extends TransactionEntityFileAccess<T, R, H>{

	private Map<Long, T> values;
	
	public TransientTransactionEntityFileAccess(EntityFileAccess<T, R, H> e,
			File file, long transactionID, byte transactionIsolation) {
		super(e, file, transactionID, transactionIsolation);
		this.offset = 0;
		this.length = 0;
		this.values = new HashMap<Long, T>();
	}

	/* transaction methods */
	
	public void setTransactionStatus(byte value) throws IOException{
		this.metadata.setTransactionStatus(value);
	}
	
	public byte getTransactionStatus() throws IOException{
		return metadata.getTransactionStatus();
	}

	public long getTransactionID() throws IOException{
		return metadata.getTransactionID();
	}

	public void setTransactionID(long value) throws IOException {
		this.metadata.setTransactionID(value);;
	}

	public void setTransactionIsolation(byte value) throws IOException{
		this.metadata.setTransactionIsolation(value);
	}
	
	public byte getTransactionIsolation() throws IOException{
		return metadata.getTransactionIsolation();
	}
	
	/* methods */
	
	public void createNewFile() throws IOException {
	}

	public void open() throws IOException {
	}
	
	@SuppressWarnings("unchecked")
	protected void write(Object entity, boolean raw) throws IOException {
		if(raw){
			ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getRecordLength());
			DataWritter dStream          = new DataWritterOutputStream(stream);
			
			super.dataHandler.writeRaw(dStream, (RawTransactionEntity<R>) entity);
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataReader readDStream          = new DataReaderInputStream(readStream);
			
			entity = super.dataHandler.read(readDStream);
		}
		
		this.values.put(this.offset, (T)entity);
		
		this.offset++;
		
		if(this.offset >= this.length){
			this.length = this.offset;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	protected void batchWrite(Object[] entities, boolean raw) throws IOException{
		
		if(raw){
			ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getRecordLength()*entities.length);
			DataWritter dStream          = new DataWritterOutputStream(stream);
			
			for(int i=0;i<entities.length;i++){
				super.dataHandler.writeRaw(dStream, (RawTransactionEntity<R>) entities[i]);
			}
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataReader readDStream          = new DataReaderInputStream(readStream);

			for(int i=0;i<entities.length;i++){
				T entity = (T)super.dataHandler.read(readDStream);
				this.values.put(offset++, entity);
			}
			
		}
		else{
			for(int i=0;i<entities.length;i++){
				this.values.put(offset++, (T) entities[i]);
			}
		}
		
		if(this.offset >= this.length){
			this.length = this.offset;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	protected Object read(boolean raw) throws IOException {
		
		if(raw){
			T e = this.values.get(this.offset);
			
			ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getRecordLength());
			DataWritter dStream          = new DataWritterOutputStream(stream);
			
			super.dataHandler.write(dStream, (TransactionalEntity<T>) e);
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataReader readDStream          = new DataReaderInputStream(readStream);

			this.offset++;
			
			return (RawTransactionEntity<R>)super.dataHandler.readRaw(readDStream);
		}
		else{
			return this.values.get(this.offset++);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	protected Object[] batchRead(int len, boolean raw) throws IOException{
		
		long maxRead    = this.length - this.offset;
		int batch       = maxRead > len? len : (int)maxRead;
		Object[] result = new TransactionalEntity[batch];
		
		for(int i=0;i<batch;i++){
			result[i] = this.values.get(this.offset + i);
		}
		
		this.offset+= batch;
		
		if(raw){
			Object[] rawRead = new RawTransactionEntity[batch];
		
			ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getRecordLength()*batch);
			DataWritter dStream          = new DataWritterOutputStream(stream);
			
			for(int i=0;i<batch;i++){
				super.dataHandler.write(dStream, (TransactionalEntity<T>) result[i]);
			}
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataReader readDStream          = new DataReaderInputStream(readStream);
			
			for(int i=0;i<batch;i++){
				rawRead[i] = super.dataHandler.readRaw(readDStream);
			}
			
			return rawRead;
		}
		else{
			return result;
		}

	}
	
	public void setLength(long value) throws IOException {
		this.length = value;
	}
	
	public void flush() throws IOException {
	}

	public boolean exists() {
		return true;
	}
	
	public void delete() throws IOException {
	}
	
}
