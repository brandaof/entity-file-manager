package org.brandao.entityfilemanager.tx;

import java.io.File;

import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.TransactionLog;

public interface EntityFileTransactionManagerConfigurer 
	extends EntityFileTransactionManager{

	void setEntityFileManagerConfigurer(EntityFileManagerConfigurer value);

	EntityFileManagerConfigurer getEntityFileManagerConfigurer();
	
	void setTransactionLog(TransactionLog value);

	TransactionLog getTransactionLog();
	
	long getTimeout();
	
	void setTimeout(long value);
	
	void setEnabledTransactionLog(boolean value);
	
	boolean isEnabledTransactionLog();
	
	File getTransactionPath();
	
	void setTransactionPath(File value);
	
	LockProvider getLockProvider();
	
	void setLockProvider(LockProvider value);
	
}
