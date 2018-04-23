package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;

import org.brandao.entityfilemanager.AbstractEntityFileAccess;
import org.brandao.entityfilemanager.EntityFileAccess;

public class TransactionEntityFileAccess<T, R> 
	extends AbstractEntityFileAccess<TransactionalEntity<T>, RawTransactionEntity<R>> {

	public static final byte TRANSACTION_NOT_STARTED 		= Byte.valueOf("00000001", 2);
	
	public static final byte TRANSACTION_STARTED_ROLLBACK 	= Byte.valueOf("00000010", 2);
	
	public static final byte TRANSACTION_ROLLEDBACK			= Byte.valueOf("00000100", 2);

	public static final byte TRANSACTION_STARTED_COMMIT 	= Byte.valueOf("00001000", 2);
	
	public static final byte TRANSACTION_COMMITED 			= Byte.valueOf("00010000", 2);
	
	public static final byte TRANSACTION_STARTED 			= Byte.valueOf("00100000", 2);
	
	private EntityFileAccess<T, R> entityFileAccess;
	
	private EntityFileTransactionDataHandler<T> entityFileTransactionDataHandler;
	
	private int transactionStatusPointer;
	
	private long transactionIDPointer;

	public TransactionEntityFileAccess(EntityFileAccess<T, R> e, long transactionID){
		super(
			getTransactionFile(e.getAbsoluteFile(), transactionID), 
			new EntityFileTransactionDataHandler<T>(e.getEntityFileDataHandler())
		);

		this.entityFileAccess                 = e;
		this.firstRecord                      = this.entityFileAccess.getFirstRecord() + 9;
		this.transactionStatusPointer         = this.entityFileAccess.getFirstRecord() + 1;
		this.transactionIDPointer             = this.transactionStatusPointer; 
		this.entityFileTransactionDataHandler = ((EntityFileTransactionDataHandler<T>)super.getEntityFileDataHandler());
		this.entityFileTransactionDataHandler.setTransactionID(transactionID);
		this.entityFileTransactionDataHandler.setTransactionStatus(TRANSACTION_NOT_STARTED);
	}

	private static File getTransactionFile(File file, long transactionID){
		String name = file.getName();
		String[] parts = name.split("\\.");
		return new File(
			file.getParentFile(), 
			parts[0] + "-" + Long.toString(transactionID, Character.MAX_RADIX) + ".txa"
		);
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
		return this.entityFileTransactionDataHandler.getTransactionStatus() != TRANSACTION_NOT_STARTED;
	}

	public long getTransactionID() throws IOException{
		return this.entityFileTransactionDataHandler.getTransactionID();
	}

	public void setTransactionID(long transactionID) throws IOException {
		this.entityFileTransactionDataHandler.setTransactionID(transactionID);
		this.fileAccess.seek(this.transactionIDPointer);
		this.fileAccess.writeLong(transactionID);
	}

}
