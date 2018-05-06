package org.brandao.entityfilemanager.tx;

import java.io.IOException;

import org.brandao.entityfilemanager.EntityFile;

public interface TransactionalEntityFile<T, R> 
	extends  EntityFile<T> {
	
	void setTransactionStatus(byte value) throws IOException;
	
	byte getTransactionStatus() throws IOException;

	void begin() throws TransactionException;
	
	void commit() throws TransactionException;
	
	void rollback() throws TransactionException;

}
