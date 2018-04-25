package org.brandao.entityfilemanager.tx;

import java.io.File;

import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.LockProvider;

public interface EntityFileTransactionManagerConfigurer 
	extends EntityFileTransactionManager{

	void setEntityFileManagerConfigurer(EntityFileManagerConfigurer value);

	EntityFileManagerConfigurer getEntityFileManagerConfigurer();
	
	long getTimeout();
	
	void setTimeout(long value);
	
	File getTransactionPath();
	
	void setTransactionPath(File value);
	
	LockProvider getLockProvider();
	
	void setLockProvider(LockProvider value);
	
}
