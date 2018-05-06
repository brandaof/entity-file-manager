package org.brandao.entityfilemanager.tx;

import java.io.IOException;

import org.brandao.entityfilemanager.AbstractEntityFileAccess;
import org.brandao.entityfilemanager.EntityFileAccess;

public class TransactionEntityFileAccess<T, R> 
	extends AbstractEntityFileAccess<TransactionalEntity<T>, RawTransactionEntity<R>> {

	private EntityFileAccess<T, R> entityFileAccess;
	
	private EntityFileTransactionDataHandler<T> entityFileTransactionDataHandler;
	
	private int transactionStatusPointer;
	
	private int transactionIDPointer;

	private int transactionIsolationPointer;
	
	public TransactionEntityFileAccess(EntityFileAccess<T, R> e, long transactionID){
		super(
			EntityFileTransactionUtil.getTransactionFile(e.getAbsoluteFile(), transactionID), 
			new EntityFileTransactionDataHandler<T>(e.getEntityFileDataHandler())
		);

		this.entityFileAccess                 = e;
		this.firstRecord                      = this.entityFileAccess.getFirstRecord() + 10;
		this.transactionStatusPointer         = this.entityFileAccess.getFirstRecord() + 1;
		this.transactionIDPointer             = this.transactionStatusPointer + 1; 
		this.transactionIsolationPointer      = this.transactionIDPointer + 8;
		this.entityFileTransactionDataHandler = ((EntityFileTransactionDataHandler<T>)super.getEntityFileDataHandler());
		this.entityFileTransactionDataHandler.setTransactionID(transactionID);
		this.entityFileTransactionDataHandler.setTransactionStatus(EntityFileTransaction.TRANSACTION_NOT_STARTED);
	}

	public void setTransactionStatus(byte value) throws IOException{
		this.entityFileTransactionDataHandler.setTransactionStatus(value);
		this.fileAccess.seek(this.transactionStatusPointer);
		this.fileAccess.writeByte(value);
	}
	
	public byte getTransactionStatus() throws IOException{
		return this.entityFileTransactionDataHandler.getTransactionStatus();
	}
	
	public boolean isStarted(){
		return this.entityFileTransactionDataHandler.getTransactionStatus() != EntityFileTransaction.TRANSACTION_NOT_STARTED;
	}

	public long getTransactionID() throws IOException{
		return this.entityFileTransactionDataHandler.getTransactionID();
	}

	public void setTransactionID(long transactionID) throws IOException {
		this.entityFileTransactionDataHandler.setTransactionID(transactionID);
		this.fileAccess.seek(this.transactionIDPointer);
		this.fileAccess.writeLong(transactionID);
	}

	public void setTransactionIsolation(byte value) throws IOException{
		this.entityFileTransactionDataHandler.setTransactionIsolation(value);
		this.fileAccess.seek(this.transactionIsolationPointer);
		this.fileAccess.writeByte(value);
	}
	
	public byte getTransactionIsolation() throws IOException{
		return this.entityFileTransactionDataHandler.getTransactionIsolation();
	}
	
	public EntityFileAccess<T, R> getEntityFileAccess() {
		return entityFileAccess;
	}
	
}
