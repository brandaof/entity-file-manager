package org.brandao.entityfilemanager;

public interface EntityFile<T> {

	long insert(T entity) throws EntityFileException;

	long insert(T[] entity) throws EntityFileException;
	
	void update(long id, T entity) throws EntityFileException;

	void update(long[] id, T[] entity) throws EntityFileException;
	
	void delete(long id) throws EntityFileException;

	void delete(long[] id) throws EntityFileException;
	
	T select(long id) throws EntityFileException;

	T[] select(long[] id) throws EntityFileException;
	
	T select(long id, boolean lock) throws EntityFileException;

	T[] select(long[] id, boolean lock) throws EntityFileException;
	
}
