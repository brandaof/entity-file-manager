package org.brandao.entityfilemanager.tx;

import org.brandao.entityfilemanager.EntityFile.Entity;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.PersistenceException;

public interface EntityFileTransactionHandler 
	extends EntityFileTransaction{

	void begin() throws TransactionException;
	
	<T> Entity<T> insert(T entity, EntityFileAccess<T> entityFileaccess) throws PersistenceException;
	
	<T> Entity<T> update(long id, T entity, EntityFileAccess<T> entityFileaccess) throws PersistenceException;
	
	<T> void delete(long id, EntityFileAccess<T> entityFileaccess) throws PersistenceException;
	
	<T> T select(long id, EntityFileAccess<T> entityFileaccess);
	
}
