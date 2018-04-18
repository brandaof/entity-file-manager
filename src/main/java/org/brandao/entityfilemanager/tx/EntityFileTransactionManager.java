package org.brandao.entityfilemanager.tx;

public interface EntityFileTransactionManager {

	EntityFileTransactionHandler getCurrent();
	
	EntityFileTransactionHandler open(String id);
	
	void close(EntityFileTransactionHandler transaction);
	
}
