package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;

import org.brandao.entityfilemanager.DataInputStream;
import org.brandao.entityfilemanager.DataOutputStream;
import org.brandao.entityfilemanager.EntityFileDataHandler;

public class EntityFileTransactionDataHandler<T, R, H> 
	implements EntityFileDataHandler<TransactionalEntity<T>, RawTransactionEntity<R>, TransactionHeader<H>>{
	
	private EntityFileDataHandler<T, R, H> handler;
	
	private Class<TransactionalEntity<T>> type;
	
	private Class<RawTransactionEntity<R>> rawType;

	private final long transactionStatusPointer;
	
	private final long transactionIDPointer;
	
	private final long transactionIsolationPointer;
	
	public EntityFileTransactionDataHandler(EntityFileDataHandler<T, R, H> handler){
		this.handler = handler;
		this.transactionStatusPointer    = this.handler.getFirstRecord();
		this.transactionIDPointer        = this.handler.getFirstRecord() + 1;
		this.transactionIsolationPointer = this.handler.getFirstRecord() + 2;
	}
	
	public void writeMetaData(DataOutputStream stream, TransactionHeader<H> value) throws IOException {
		this.handler.writeMetaData(stream, value.getParent());
		stream.writeByte(value.getTransactionStatus());
		stream.writeLong(value.getTransactionID());
		stream.writeByte(value.getTransactionIsolation());
	}

	@SuppressWarnings("unchecked")
	public TransactionHeader<H> readMetaData(DataInputStream stream) throws IOException {
		
		H parent = this.handler.readMetaData(stream);
		
		TransactionHeader<H> result = new TransactionHeader<H>(parent);
		result.setTransactionStatus(stream.readByte());
		result.setTransactionID(stream.readLong());
		result.setTransactionIsolation(stream.readByte());

		ParameterizedType ptype = (ParameterizedType)this.getClass().getGenericInterfaces()[0];
		this.type    = (Class<TransactionalEntity<T>>) ptype.getActualTypeArguments()[0];
		this.rawType = (Class<RawTransactionEntity<R>>) ptype.getActualTypeArguments()[1];
		
		return result;
	}

	public void writeEOF(DataOutputStream stream) throws IOException {
		this.handler.writeEOF(stream);
	}

	public void write(DataOutputStream stream, TransactionalEntity<T> entity)
			throws IOException {
		stream.writeLong(entity.getRecordID());
		stream.writeByte(entity.getFlags());
		this.handler.write(stream, entity.getEntity());
	}

	public TransactionalEntity<T> read(DataInputStream stream)
			throws IOException {
		
		long recordID = stream.readLong();
		byte flags    = stream.readByte();
		T entity      = this.handler.read(stream);
		return new TransactionalEntity<T>(recordID, flags, entity);
	}

	public void writeRaw(DataOutputStream stream, RawTransactionEntity<R> entity)
			throws IOException {
		stream.writeLong(entity.getRecordID());
		stream.writeByte(entity.getFlags());
		this.handler.writeRaw(stream, entity.getEntity());
		
	}

	public RawTransactionEntity<R> readRaw(DataInputStream stream)
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

}
