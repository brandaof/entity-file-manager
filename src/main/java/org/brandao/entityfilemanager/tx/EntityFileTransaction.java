package org.brandao.entityfilemanager.tx;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.PersistenceException;

public interface EntityFileTransaction {

	public static final byte TRANSACTION_NOT_STARTED 		= Byte.valueOf("00000001", 2);
	
	public static final byte TRANSACTION_STARTED 			= Byte.valueOf("00000010", 2);
	
	public static final byte TRANSACTION_STARTED_ROLLBACK 	= Byte.valueOf("00000100", 2);
	
	public static final byte TRANSACTION_ROLLEDBACK			= Byte.valueOf("00001000", 2);

	public static final byte TRANSACTION_STARTED_COMMIT 	= Byte.valueOf("00010000", 2);
	
	public static final byte TRANSACTION_COMMITED 			= Byte.valueOf("00100000", 2);
	
	<T,R> long insert(T entity, EntityFileAccess<T,R> entityFileaccess) throws PersistenceException;
	
	<T,R> void update(long id, T entity, EntityFileAccess<T,R> entityFileaccess) throws PersistenceException;
	
	<T,R> void delete(long id, EntityFileAccess<T,R> entityFileaccess) throws PersistenceException;
	
	<T,R> T select(long id, EntityFileAccess<T,R> entityFileaccess) throws PersistenceException;
	
	byte getStatus();
	
	boolean isRolledBack();
	
	boolean isCommited();
	
	boolean isClosed();
	
	void rollback() throws TransactionException;
	
	void commit() throws TransactionException;

}
