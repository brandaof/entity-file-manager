package org.brandao.entityfilemanager.tx;

public interface EntityFileTransactionManager {

	long getNextTransactionID();
	
	EntityFileTransactionHandler getCurrent();
	
	EntityFileTransactionHandler open(String id);
	
	void close(EntityFileTransactionHandler transaction);
	
}
