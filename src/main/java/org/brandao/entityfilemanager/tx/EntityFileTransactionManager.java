package org.brandao.entityfilemanager.tx;

public interface EntityFileTransactionManager {

	EntityFileTransaction create();
	
	void close(EntityFileTransaction tx);
	
}
