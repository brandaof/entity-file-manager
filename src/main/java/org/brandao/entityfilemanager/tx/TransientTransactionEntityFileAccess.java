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
		this.lock   = e.getLock();
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
	protected void write(Object[] b, int off, int len, boolean raw) throws IOException{
		
		int last = off + len;
		
		if(raw){
			ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getRecordLength()*len);
			DataWritter dStream          = new DataWritterOutputStream(stream);
			
			for(int i=off;i<last;i++){
				super.dataHandler.writeRaw(dStream, (RawTransactionEntity<R>) b[i]);
			}
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataReader readDStream          = new DataReaderInputStream(readStream);

			for(int i=0;i<len;i++){
				T entity = (T)super.dataHandler.read(readDStream);
				this.values.put(offset++, entity);
			}
			
		}
		else{
			for(int i=off;i<last;i++){
				this.values.put(offset++, (T) b[i]);
			}
		}
		
		if(this.offset >= this.length){
			this.length = this.offset;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	protected int read(Object[] b, int off, int len, boolean raw) throws IOException{
		
		long maxRead    = this.length - this.offset;
		int batch       = maxRead > len? len : (int)maxRead;
		int last        = off + batch;
		
		if(raw){
			
			ByteArrayOutputStream stream = new ByteArrayOutputStream(this.dataHandler.getRecordLength()*batch);
			DataWritter dStream          = new DataWritterOutputStream(stream);

			for(int i=0;i<batch;i++){
				super.dataHandler.write(dStream, (TransactionalEntity<T>)values.get(offset + i));
			}
			
			ByteArrayInputStream readStream = new ByteArrayInputStream(stream.toByteArray());
			DataReader readDStream          = new DataReaderInputStream(readStream);
			
			for(int i=off;i<last;i++){
				b[i] = super.dataHandler.readRaw(readDStream);
			}
			
		}
		else{
			
			for(int i=off;i<last;i++){
				b[i] = this.values.get(this.offset + i);
			}
			
		}

		this.offset+= batch;
		
		return len;
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
