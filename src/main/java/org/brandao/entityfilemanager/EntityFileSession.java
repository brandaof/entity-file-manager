package org.brandao.entityfilemanager;

import org.brandao.entityfilemanager.tx.EntityFileTransaction;

public interface EntityFileSession {

	EntityFileTransaction beginTransaction();
	
	Object select(long offset, String typeName) throws PersistenceException;

	Object select(String id, String typeName) throws PersistenceException;
	
	long insert(Object value, String typeName) throws PersistenceException;

	long insert(Object value, String id, String typeName) throws PersistenceException;
	
	void update(Object value, long offset, String typeName) throws PersistenceException;

	void update(Object value, String id, String typeName) throws PersistenceException;
	
	void remove(long offset, String typeName) throws PersistenceException;

	void remove(String id, String typeName) throws PersistenceException;
	
	long getLength(String typeName) throws PersistenceException;
	
	void close();
	
}
