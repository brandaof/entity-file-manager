package org.brandao.entityfilemanager;

import java.io.IOException;

public interface EntityFile<T> {

	Entity<T> insert(T entity) throws IOException;
	
	Entity<T> update(long id, T entity) throws IOException;
	
	void delete(long id) throws IOException;
	
	T select(long id) throws IOException;

	public interface Entity<T>{
		
		long getRow();
		
		T getEntity();
		
	}
}
