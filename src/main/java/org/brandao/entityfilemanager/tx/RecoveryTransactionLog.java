package org.brandao.entityfilemanager.tx;

public interface RecoveryTransactionLog {

	void setLimitFileLength(long value) throws TransactionException;
	
	long getLimitFileLength() throws TransactionException;
	
	void close() throws TransactionException;
	
	void registerTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException;
	
	void deleteTransaction(ConfigurableEntityFileTransaction ceft)
			throws TransactionException;
	
	void open() throws TransactionException;
	
}
