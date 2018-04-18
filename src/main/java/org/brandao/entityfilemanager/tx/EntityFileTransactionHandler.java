package org.brandao.entityfilemanager.tx;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.PersistenceException;

public interface EntityFileTransactionHandler 
	extends EntityFileTransaction{

	void begin() throws TransactionException;
	
	<T> long insert(T entity, EntityFileAccess<T> entityFile) throws PersistenceException;
	
	<T> long update(long id, T entity, EntityFileAccess<T> entityFile) throws PersistenceException;
	
	<T> void delete(long id, EntityFileAccess<T> entityFile) throws PersistenceException;
	
	<T> T select(long id, EntityFileAccess<T> entityFile);
	
}
