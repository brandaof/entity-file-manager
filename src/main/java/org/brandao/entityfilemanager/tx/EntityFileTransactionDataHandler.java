package org.brandao.entityfilemanager.tx;

import java.io.IOException;

import org.brandao.entityfilemanager.DataReader;
import org.brandao.entityfilemanager.DataWritter;
import org.brandao.entityfilemanager.EntityFileDataHandler;

public class EntityFileTransactionDataHandler<T, R, H> 
	implements EntityFileDataHandler<TransactionalEntity<T>, RawTransactionEntity<R>, TransactionHeader<H>>{
	
	private EntityFileDataHandler<T, R, H> handler;
	
	private Class<TransactionalEntity<T>> type;
	
	private Class<RawTransactionEntity<R>> rawType;

	private final long transactionStatusPointer;
	
	private final long transactionIDPointer;
	
	private final long transactionIsolationPointer;
	
	@SuppressWarnings("unchecked")
	public EntityFileTransactionDataHandler(EntityFileDataHandler<T, R, H> handler){
		this.handler = handler;
		this.transactionStatusPointer    = this.handler.getFirstRecord();
		this.transactionIDPointer        = this.handler.getFirstRecord() + 1;
		this.transactionIsolationPointer = this.handler.getFirstRecord() + 2;
		this.type = (Class<TransactionalEntity<T>>)new TransactionalEntity<T>(0,(byte)0,null).getClass();
		this.rawType = (Class<RawTransactionEntity<R>>)new RawTransactionEntity<R>(0,(byte)0,null).getClass();
	}
	
	public void writeMetaData(DataWritter stream, TransactionHeader<H> value) throws IOException {
		this.handler.writeMetaData(stream, value.getParent());
		stream.writeByte(value.getTransactionStatus());
		stream.writeLong(value.getTransactionID());
		stream.writeByte(value.getTransactionIsolation());
	}

	public TransactionHeader<H> readMetaData(DataReader stream) throws IOException {
		
		H parent = this.handler.readMetaData(stream);
		
		TransactionHeader<H> result = new TransactionHeader<H>(parent);
		result.setTransactionStatus(stream.readByte());
		result.setTransactionID(stream.readLong());
		result.setTransactionIsolation(stream.readByte());
		
		return result;
	}

	public void writeEOF(DataWritter stream) throws IOException {
		this.handler.writeEOF(stream);
	}

	public void write(DataWritter stream, TransactionalEntity<T> entity)
			throws IOException {
		stream.writeLong(entity.getRecordID());
		stream.writeByte(entity.getFlags());
		this.handler.write(stream, entity.getEntity());
	}

	public TransactionalEntity<T> read(DataReader stream)
			throws IOException {
		
		long recordID = stream.readLong();
		byte flags    = stream.readByte();
		T entity      = this.handler.read(stream);
		return new TransactionalEntity<T>(recordID, flags, entity);
	}

	public void writeRaw(DataWritter stream, RawTransactionEntity<R> entity)
			throws IOException {
		stream.writeLong(entity.getRecordID());
		stream.writeByte(entity.getFlags());
		this.handler.writeRaw(stream, entity.getEntity());
		
	}

	public RawTransactionEntity<R> readRaw(DataReader stream)
			throws IOException {
		long recordID = stream.readLong();
		byte flags    = stream.readByte();
		R entity      = this.handler.readRaw(stream);
		return new RawTransactionEntity<R>(recordID, flags, entity);
	}

	public int getRecordLength() {
		return this.handler.getRecordLength() + 9;
	}

	public int getEOFLength() {
		return 1;
	}

	public int getFirstRecord() {
		return this.handler.getFirstRecord() + 10;
	}

	public Class<TransactionalEntity<T>> getType() {
		return this.type;
	}

	public Class<RawTransactionEntity<R>> getRawType() {
		return this.rawType;
	}

	public long getTransactionStatusPointer() {
		return transactionStatusPointer;
	}

	public long getTransactionIDPointer() {
		return transactionIDPointer;
	}

	public long getTransactionIsolationPointer() {
		return transactionIsolationPointer;
	}

	public int getHeaderLength() {
		return (int)this.getFirstRecord();
	}

	public long getFirstPointer() {
		return 0;
	}

}
