package org.brandao.entityfilemanager.tx;

public interface EntityFileTransactionManager {

	void init() throws TransactionException;
	
	void destroy() throws TransactionException;
	
	EntityFileTransaction openTransaction() throws TransactionException;

	void closeTransaction(EntityFileTransaction tx) throws TransactionException;
	
}
