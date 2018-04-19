package org.brandao.entityfilemanager.tx;

import java.io.IOException;

import org.brandao.entityfilemanager.DataInputStream;
import org.brandao.entityfilemanager.DataOutputStream;
import org.brandao.entityfilemanager.EntityFileDataHandler;

public class EntityFileTransactionDataHandler<T> 
	implements EntityFileDataHandler<TransactionalEntity<T>>{
	
	private EntityFileDataHandler<T> handler;
	
	private byte transactionStatus;
	
	private long transactionID;
	
	public EntityFileTransactionDataHandler(EntityFileDataHandler<T> handler){
		this.handler = handler;
	}
	
	public void writeMetaData(DataOutputStream stream) throws IOException {
		this.handler.writeMetaData(stream);
		stream.writeByte(this.transactionStatus);
		stream.writeLong(this.transactionID);
	}

	public void readMetaData(DataInputStream stream) throws IOException {
		this.handler.readMetaData(stream);
		this.transactionStatus = stream.readByte();
		this.transactionID = stream.readLong();
	}

	public void writeEOF(DataOutputStream stream) throws IOException {
		this.handler.writeEOF(stream);
	}

	public void write(DataOutputStream stream, TransactionalEntity<T> entity)
			throws IOException {
	}

	public TransactionalEntity<T> read(DataInputStream stream)
			throws IOException {
		return null;
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

}
