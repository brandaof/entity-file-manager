package org.brandao.entityfilemanager.tx;

import org.brandao.entityfilemanager.EntityFileAccess;

public interface EntityFileTransactionManager {

	void init() throws TransactionException;
	
	void destroy() throws TransactionException;
	
	EntityFileTransaction openTransaction() throws TransactionException;

	<T,R,H> TransactionEntity<T,R> createTransactionalEntityFile(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	byte transactionIsolation
			) throws TransactionException;
			
	void closeTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
	void commitTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
	void rollbackTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
}
