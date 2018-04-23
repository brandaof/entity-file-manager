package org.brandao.entityfilemanager.tx;

import java.io.IOException;

import org.brandao.entityfilemanager.AbstractEntityFileAccess;
import org.brandao.entityfilemanager.EntityFileAccess;

public class TransactionEntityFileAccess<T, R> 
	extends AbstractEntityFileAccess<TransactionalEntity<T>, RawTransactionEntity<R>> {

	public static final byte TRANSACTION_NOT_STARTED 		= Byte.valueOf("00000001", 2);
	
	public static final byte TRANSACTION_STARTED_ROLLBACK 	= Byte.valueOf("00000010", 2);
	
	public static final byte TRANSACTION_ROLLBACK			= Byte.valueOf("00000100", 2);

	public static final byte TRANSACTION_STARTED_COMMIT 	= Byte.valueOf("00001000", 2);
	
	public static final byte TRANSACTION_COMMITED 			= Byte.valueOf("00010000", 2);
	
	public static final byte TRANSACTION_STARTED 			= Byte.valueOf("00100000", 2);
	
	private EntityFileAccess<T, R> entityFileAccess;
	
	private EntityFileTransactionDataHandler<T> entityFileTransactionDataHandler;
	
	private int transactionStatusPointer;
	
	private byte transactionStatus;
	
	public TransactionEntityFileAccess(EntityFileAccess<T, R> e, long transactionID){
		super(e.getAbsoluteFile(), new EntityFileTransactionDataHandler<T>(e.getEntityFileDataHandler()));

		this.entityFileAccess                 = e;
		this.firstRecord                      = this.entityFileAccess.getFirstRecord() + 9;
		this.transactionStatusPointer         = this.entityFileAccess.getFirstRecord();
		this.entityFileTransactionDataHandler = ((EntityFileTransactionDataHandler<T>)super.getEntityFileDataHandler());
	}

	public void setTransactionStatus(byte value) throws IOException{
	}
	
	public byte getTransactionStatus() throws IOException{
		return -1;
	}
	
	public boolean isStarted(){
		return this.entityFileTransactionDataHandler.getTransactionStatus() != TRANSACTION_NOT_STARTED;
	}

}
