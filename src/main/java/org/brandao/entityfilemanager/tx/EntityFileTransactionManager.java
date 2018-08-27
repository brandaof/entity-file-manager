package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;

public interface EntityFileTransactionManager {

	void init() throws TransactionException;
	
	void destroy() throws TransactionException;
	
	EntityFileTransaction openTransaction() throws TransactionException;

	<T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	byte transactionIsolation
			) throws TransactionException;
			
	<T,R,H> TransactionEntity<T,R> createTransactionalEntity(
			TransactionEntityFileAccess<T,R,H> transactionEntityFile, long transactionID,
			byte transactionIsolation, long timeout) throws TransactionException;
	
	<T,R,H> TransactionEntityFileAccess<T, R, H> createTransactionEntityFileAccess(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	
			byte transactionIsolation) throws TransactionException;
	
	ConfigurableEntityFileTransaction load(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> transactionFiles,
			byte status, long transactionID, byte transactionIsolation, boolean started, 
			boolean rolledBack,	boolean commited, long timeout) throws TransactionException;
	
	void closeTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
	void commitTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
	void rollbackTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
}
