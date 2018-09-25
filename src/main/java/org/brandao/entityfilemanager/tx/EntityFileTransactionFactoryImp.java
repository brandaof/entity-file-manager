package org.brandao.entityfilemanager.tx;

import java.io.File;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileTransactionFactory;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.tx.readcommited.ReadCommitedTransactionalEntityFile;

public class EntityFileTransactionFactoryImp 
	implements EntityFileTransactionFactory{

	public EntityFileTransactionFactoryImp(){
	}
	
	public <T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	
			byte transactionIsolation, LockProvider lockProvider, long timeout) throws TransactionException{
		
		if(transactionIsolation != EntityFileTransaction.TRANSACTION_READ_COMMITED){
			throw new TransactionException("transaction not supported: " + transactionIsolation);
		}
		
		TransactionEntityFileAccess<T, R, H> txFile =
					createTransactionEntityFileAccess(entityFile, transactionID, transactionIsolation);
				
		return new ReadCommitedTransactionalEntityFile<T, R, H>(txFile, lockProvider, timeout);
	}

	public <T,R,H> TransactionEntityFileAccess<T, R, H> createTransactionEntityFileAccess(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	byte transactionIsolation) throws TransactionException{
		
		try{
			TransactionEntityFileAccess<T, R, H> tefa =
				new TransientTransactionEntityFileAccess<T, R, H>(
						entityFile, 
						new File(entityFile.getAbsolutePath() + "_" + Long.toString(transactionID, Character.MAX_RADIX)), 
						transactionID, 
						transactionIsolation);
			
			tefa.createNewFile();
			return tefa;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}
	
	public <T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			TransactionEntityFileAccess<T,R,H> transactionEntityFile, long transactionID,
			byte transactionIsolation, LockProvider lockProvider, long timeout) throws TransactionException{
		
		if(transactionIsolation != EntityFileTransaction.TRANSACTION_READ_COMMITED){
			throw new TransactionException("transaction not supported: " + transactionIsolation);
		}
		
		return new ReadCommitedTransactionalEntityFile<T, R, H>(
					transactionEntityFile, lockProvider, timeout < 0? timeout : timeout);
	}
	

}
