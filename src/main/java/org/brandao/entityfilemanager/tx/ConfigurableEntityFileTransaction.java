package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.LockProvider;

public interface ConfigurableEntityFileTransaction extends EntityFileTransaction{

	void setEntityFileTransactionManagerConfigurer(EntityFileTransactionManagerConfigurer entityFileTransactionManager);
	
	void setLockProvider(LockProvider lockProvider);
	
	void setStatus(byte value);
	
	void setRolledBack(boolean value);
	
	void setCommited(boolean value);
	
	void setClosed(boolean value);
	
	void setStarted(boolean value);
	
	void setTransactionID(byte value);
	
	void setDirty(boolean value);
	
	boolean isRecoveredTransaction();
	
	boolean isDirty();
	
	boolean isStarted();

	long getTransactionID();
	
	Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> getTransactionFiles();
	
}
