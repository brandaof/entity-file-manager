package org.brandao.entityfilemanager;

import org.brandao.entityfilemanager.tx.EntityFileTransaction;
import org.brandao.entityfilemanager.tx.TransactionException;

public interface EntityFileManager {

	EntityFileTransaction beginTransaction() throws TransactionException;
	
	<T> EntityFile<T> getEntityFile(String name, 
			EntityFileTransaction entityFileTransaction, Class<T> type);
	
}
