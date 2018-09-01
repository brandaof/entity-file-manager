package org.brandao.entityfilemanager;

import org.brandao.entityfilemanager.tx.ConfigurableEntityFileTransaction;
import org.brandao.entityfilemanager.tx.TransactionException;

public interface TransactionLog {

	void setLimitFileLength(long value) throws TransactionException;
	
	long getLimitFileLength();
	
	void registerLog(ConfigurableEntityFileTransaction ceft) throws TransactionException;
	
	void open() throws TransactionException;
	
}
