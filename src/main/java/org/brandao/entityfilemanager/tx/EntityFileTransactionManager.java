package org.brandao.entityfilemanager.tx;

public interface EntityFileTransactionManager {

	void init() throws TransactionException;
	
	void destroy() throws TransactionException;
	
	EntityFileTransaction openTransaction() throws TransactionException;

	void closeTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
	void commitTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
	void rollbackTransaction(ConfigurableEntityFileTransaction tx) throws TransactionException;
	
}
