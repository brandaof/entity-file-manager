package org.brandao.entityfilemanager;

import java.io.IOException;

public interface EntityFile<T> {

	long insert(T entity) throws IOException;
	
	void update(long id, T entity) throws IOException;
	
	void delete(long id) throws IOException;
	
	T select(long id) throws IOException;

}
