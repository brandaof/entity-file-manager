package org.brandao.entityfilemanager.tx;

public interface RecoveryTransactionLog {

	void setForceReload(boolean value);

	boolean isForceReload();
	
	void setLimitFileLength(long value) throws TransactionException;
	
	long getLimitFileLength() throws TransactionException;
	
	void close() throws TransactionException;
	
	void registerTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException;
	
	void deleteTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException;
	
	void open() throws TransactionException;
	
}
