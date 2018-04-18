package org.brandao.entityfilemanager.tx;

public interface EntityFileTransaction {

	boolean isRolledBack();
	
	boolean isCommited();
	
	void rollback() throws TransactionException;
	
	void commit() throws TransactionException;

}
