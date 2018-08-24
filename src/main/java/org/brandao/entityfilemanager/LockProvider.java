package org.brandao.entityfilemanager;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public interface LockProvider 
	extends Serializable{

	void lock(EntityFileAccess<?, ?, ?> entityFile) throws LockException;

	boolean tryLock(EntityFileAccess<?, ?, ?> entityFile, long unit, TimeUnit timeunit) throws LockException;
	
	void unlock(EntityFileAccess<?, ?, ?> entityFile) throws LockException;
	
	void lock(EntityFileAccess<?, ?, ?> entityFile, long pointer) throws LockException;

	boolean tryLock(EntityFileAccess<?, ?, ?> entityFile, long pointer, long unit, TimeUnit timeunit) throws LockException;
	
	void unlock(EntityFileAccess<?, ?, ?> entityFile, long pointer) throws LockException;
	
}
