package org.brandao.entityfilemanager;

import org.brandao.entityfilemanager.tx.EntityFileTransaction;

public interface EntityFileManager {

	EntityFileTransaction beginTransaction();
	
	<T> EntityFile<T> getEntityFile(String name);
	
}
