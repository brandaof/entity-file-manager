package org.brandao.entityfilemanager.tx.async;

import java.io.File;
import java.util.concurrent.ConcurrentMap;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileTransactionFactory;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.tx.Await;
import org.brandao.entityfilemanager.tx.EntityFileTransaction;
import org.brandao.entityfilemanager.tx.TransactionEntity;
import org.brandao.entityfilemanager.tx.TransactionEntityFileAccess;
import org.brandao.entityfilemanager.tx.TransactionException;
import org.brandao.entityfilemanager.tx.TransientTransactionEntityFileAccess;
import org.brandao.entityfilemanager.tx.readcommited.ReadCommitedTransactionalEntityFile;

public class AsyncEntityFileTransactionFactory 
	implements EntityFileTransactionFactory{

	private ConcurrentMap<EntityFileAccess<?,?,?>, AutoFlushVirutalEntityFileAccess<?, ?, ?>> efam;
	
	public AsyncEntityFileTransactionFactory(AsyncRecoveryTransactionLog asyncRecoveryTransactionLog){
		efam = asyncRecoveryTransactionLog.efam;
	}
	
	@SuppressWarnings("unchecked")
	public <T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	
			byte transactionIsolation, LockProvider lockProvider, long timeout) throws TransactionException{
		
		if(transactionIsolation != EntityFileTransaction.TRANSACTION_READ_COMMITED){
			throw new TransactionException("transaction not supported: " + transactionIsolation);
		}
		
		AutoFlushVirutalEntityFileAccess<T,R,H> afvefa = 
				(AutoFlushVirutalEntityFileAccess<T, R, H>) efam.get(entityFile);
		
		try{
			if(afvefa == null){
				synchronized (efam) {
					afvefa = 
							(AutoFlushVirutalEntityFileAccess<T, R, H>) efam.get(entityFile);
					
					if(afvefa == null){
						afvefa = new AutoFlushVirutalEntityFileAccess<T, R, H>(
								entityFile, 
								new File(entityFile.getAbsolutePath() + "_virtual"),
								new Await());
						afvefa.createNewFile();
						efam.putIfAbsent(entityFile, afvefa);
					}
				}
			}
			
			TransactionEntityFileAccess<T, R, H> txFile =
					new AwaitTransactionEntityFileAccess<T, R, H>(
							afvefa, 
							new File(entityFile.getAbsolutePath() + "_" + Long.toString(transactionID, Character.MAX_RADIX)), 
							transactionID, 
							transactionIsolation, afvefa.getAwait());
				
			txFile.createNewFile();
			
			return 
				new ReadCommitedTransactionalEntityFile<T, R, H>(
						txFile, lockProvider, timeout);
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
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
