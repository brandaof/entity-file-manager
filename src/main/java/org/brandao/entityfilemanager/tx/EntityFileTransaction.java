package org.brandao.entityfilemanager.tx;

import java.io.Serializable;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;

public interface EntityFileTransaction extends Serializable{

	/* Transaction isolation */
	
	public static final byte TRANSACTION_READ_COMMITED 		= Byte.valueOf("00000001", 2);
	
	/* Transaction status */
	
	public static final byte TRANSACTION_NOT_STARTED 		= Byte.valueOf("00000001", 2);
	
	public static final byte TRANSACTION_STARTED 			= Byte.valueOf("00000010", 2);
	
	public static final byte TRANSACTION_STARTED_ROLLBACK 	= Byte.valueOf("00000100", 2);
	
	public static final byte TRANSACTION_ROLLEDBACK			= Byte.valueOf("00001000", 2);

	public static final byte TRANSACTION_STARTED_COMMIT 	= Byte.valueOf("00010000", 2);
	
	public static final byte TRANSACTION_COMMITED 			= Byte.valueOf("00100000", 2);
	
	<T,R,H> long insert(T entity, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;

	<T,R,H> long[] insert(T[] entity, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;
	
	<T,R,H> void update(long id, T entity, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;

	<T,R,H> void update(long[] id, T[] entity, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;
	
	<T,R,H> void delete(long id, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;

	<T,R,H> void delete(long[] id, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;
	
	<T,R,H> T select(long id, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;

	<T,R,H> T[] select(long[] id, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;
	
	<T,R,H> T select(long id, boolean lock, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;

	<T,R,H> T[] select(long[] id, boolean lock, EntityFileAccess<T, R, H> entityFileAccess) throws EntityFileException;
	
	void setTimeout(long value);
	
	long getTimeout();
	
	byte getStatus();
	
	byte getTransactionIsolation();
	
	boolean isRolledBack();
	
	boolean isCommited();
	
	boolean isClosed();
	
	void rollback() throws TransactionException;
	
	void commit() throws TransactionException;

}
