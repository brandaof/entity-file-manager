package org.brandao.entityfilemanager.tx;

import java.io.File;

import org.brandao.entityfilemanager.AbstractEntityFileAccess;
import org.brandao.entityfilemanager.EntityFileAccess;

public class EntityFileAccessTransaction<T> 
	extends AbstractEntityFileAccess<TransactionalEntity<T>> {

	public static final byte TRANSACTION_NOT_STARTED 		= Byte.valueOf("00000001", 2);
	
	public static final byte TRANSACTION_STARTED_ROLLBACK 	= Byte.valueOf("00000010", 2);
	
	public static final byte TRANSACTION_ROLLBACK			= Byte.valueOf("00000100", 2);

	public static final byte TRANSACTION_STARTED_COMMIT 	= Byte.valueOf("00001000", 2);
	
	public static final byte TRANSACTION_COMMITED 			= Byte.valueOf("00010000", 2);
	
	private EntityFileAccess<T> entityFileAccess;
	
	private EntityFileTransactionDataHandler<T> entityFileTransactionDataHandler;
	
	public EntityFileAccessTransaction(File file,
			EntityFileAccess<T> e, long transactionID){
		super(file, new EntityFileTransactionDataHandler<T>(e.getEntityFileDataHandler()));

		this.entityFileAccess = e;
		this.firstRecord = this.entityFileAccess.getFirstRecord() + 9;
		this.entityFileTransactionDataHandler = ((EntityFileTransactionDataHandler<T>)super.getEntityFileDataHandler());
		
		this.entityFileTransactionDataHandler.setTransactionStatus(TRANSACTION_NOT_STARTED);
		this.entityFileTransactionDataHandler.setTransactionID(transactionID);
	}

}
