package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;

import org.brandao.entityfilemanager.DataInputStream;
import org.brandao.entityfilemanager.DataOutputStream;
import org.brandao.entityfilemanager.EntityFileDataHandler;

public class EntityFileTransactionDataHandler<T, R, H> 
	implements EntityFileDataHandler<TransactionalEntity<T>, RawTransactionEntity<R>, TransactionHeader<H>>{
	
	private EntityFileDataHandler<T, R, H> handler;
	
	private byte transactionStatus;
	
	private long transactionID;
	
	private byte transactionIsolation;
	
	public EntityFileTransactionDataHandler(EntityFileDataHandler<T, R, H> handler){
		this.handler = handler;
	}
	
	public void writeMetaData(DataOutputStream stream, TransactionHeader<H> value) throws IOException {
		this.handler.writeMetaData(stream, value.getParent());
		stream.writeByte(this.transactionStatus);
		stream.writeLong(this.transactionID);
		stream.writeByte(this.transactionIsolation);
	}

	public TransactionHeader<H> readMetaData(DataInputStream stream) throws IOException {
		this.handler.readMetaData(stream);
		this.transactionStatus = stream.readByte();
		this.transactionID = stream.readLong();
		return null;
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

	public byte getTransactionStatus() {
		return transactionStatus;
	}

	public void setTransactionStatus(byte transactionStatus) {
		this.transactionStatus = transactionStatus;
	}

	public long getTransactionID() {
		return transactionID;
	}

	public void setTransactionID(long transactionID) {
		this.transactionID = transactionID;
	}

	public byte getTransactionIsolation() {
		return transactionIsolation;
	}

	public void setTransactionIsolation(byte transactionIsolation) {
		this.transactionIsolation = transactionIsolation;
	}

	@SuppressWarnings("unchecked")
	public Class<TransactionalEntity<T>> getType() {
		return (Class<TransactionalEntity<T>>)((ParameterizedType)this.getClass()
				.getGenericInterfaces()[0]).getActualTypeArguments()[0];
	}

}
