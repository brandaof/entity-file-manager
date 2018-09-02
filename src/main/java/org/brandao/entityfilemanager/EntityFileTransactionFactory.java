package org.brandao.entityfilemanager;

import org.brandao.entityfilemanager.tx.TransactionEntity;
import org.brandao.entityfilemanager.tx.TransactionEntityFileAccess;
import org.brandao.entityfilemanager.tx.TransactionException;

public interface EntityFileTransactionFactory {

	<T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	
			byte transactionIsolation, LockProvider lockProvider, long timeout) throws TransactionException;
	
	<T,R,H> TransactionEntityFileAccess<T, R, H> createTransactionEntityFileAccess(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	
			byte transactionIsolation) throws TransactionException;
	
	<T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			TransactionEntityFileAccess<T,R,H> transactionEntityFile, long transactionID,
			byte transactionIsolation, LockProvider lockProvider, long timeout) throws TransactionException;
	
}
