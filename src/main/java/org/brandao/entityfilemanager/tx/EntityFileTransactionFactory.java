package org.brandao.entityfilemanager.tx;

public interface EntityFileTransactionFactory {

	EntityFileTransactionHandler getCurrent();
	
	EntityFileTransactionHandler open(String id);
	
	void close(EntityFileTransactionHandler transaction);
	
}
