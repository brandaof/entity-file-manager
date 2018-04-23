package org.brandao.entityfilemanager.tx;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.PersistenceException;

public interface EntityFileTransaction {

	<T,R> long insert(T entity, EntityFileAccess<T,R> entityFileaccess) throws PersistenceException;
	
	<T,R> void update(long id, T entity, EntityFileAccess<T,R> entityFileaccess) throws PersistenceException;
	
	<T,R> void delete(long id, EntityFileAccess<T,R> entityFileaccess) throws PersistenceException;
	
	<T,R> T select(long id, EntityFileAccess<T,R> entityFileaccess) throws PersistenceException;
	
	boolean isRolledBack();
	
	boolean isCommited();
	
	void rollback() throws TransactionException;
	
	void commit() throws TransactionException;

}
