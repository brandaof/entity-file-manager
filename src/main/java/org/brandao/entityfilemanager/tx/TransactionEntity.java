package org.brandao.entityfilemanager.tx;

import java.io.IOException;

import org.brandao.entityfilemanager.EntityFile;
import org.brandao.entityfilemanager.EntityFileAccess;

public interface TransactionEntity<T, R> 
	extends EntityFile<T> {
	
	void setTransactionStatus(byte value) throws IOException;
	
	byte getTransactionStatus() throws IOException;

	byte getTransactionIsolation() throws IOException;
	
	TransactionEntityFileAccess<T,R,?> getTransactionEntityFileAccess();
	
	EntityFileAccess<T,R,?> getEntityFileAccess();
	
	void begin() throws TransactionException;
	
	void commit() throws TransactionException;
	
	void rollback() throws TransactionException;

	void close() throws IOException;
	
	void delete() throws IOException;
	
	void releaseLocks();
	
}
