package org.brandao.entityfilemanager.tx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.brandao.entityfilemanager.DataInputStream;
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
			DataWritterOutputStream dStream     = new DataWritterOutputStream(stream);
			
			super.dataHandler.writeRaw(dStream, (RawTransactionEntity<R>) entity);
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataInputStream readDStream     = new DataInputStream(readStream);
			
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
			DataWritterOutputStream dStream     = new DataWritterOutputStream(stream);
			
			for(int i=0;i<entities.length;i++){
				super.dataHandler.writeRaw(dStream, (RawTransactionEntity<R>) entities[i]);
			}
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataInputStream readDStream     = new DataInputStream(readStream);

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
			DataWritterOutputStream dStream     = new DataWritterOutputStream(stream);
			
			super.dataHandler.write(dStream, (TransactionalEntity<T>) e);
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataInputStream readDStream     = new DataInputStream(readStream);

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
			DataWritterOutputStream dStream     = new DataWritterOutputStream(stream);
			
			for(int i=0;i<batch;i++){
				super.dataHandler.write(dStream, (TransactionalEntity<T>) result[i]);
			}
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataInputStream readDStream     = new DataInputStream(readStream);
			
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

	public void close() throws IOException {
	}

	public boolean exists() {
		return true;
	}
	
	public void delete() throws IOException {
	}

	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeObject(metadata);
		stream.writeUTF(file.getName());
		stream.writeInt(batchLength);
		stream.writeObject(dataHandler);
		//stream.writeObject(parent);
		stream.writeObject(this.values);
    }

    @SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    	metadata               = (TransactionHeader<H>) stream.readObject();
		file                   = new File(stream.readUTF());
		batchLength            = stream.readInt();
		dataHandler            = (EntityFileTransactionDataHandler<T,R,H>) stream.readObject();
		//parent                 = (EntityFileAccess<T, R, H>)stream.readObject();
		values                 = (Map<Long, T>) stream.readObject();
		
		transactionDataHandler = (EntityFileTransactionDataHandler<T,R,H>)dataHandler;
		
		this.lock              = new ReentrantLock();
    }
	
}
