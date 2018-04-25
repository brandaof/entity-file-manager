package org.brandao.entityfilemanager;

import java.util.concurrent.TimeUnit;

public interface LockProvider {

	void lock(EntityFileAccess<?, ?> entityFile);

	boolean tryLock(EntityFileAccess<?, ?> entityFile, long unit, TimeUnit timeunit);
	
	void unlock(EntityFileAccess<?, ?> entityFile);
	
	void lock(EntityFileAccess<?, ?> entityFile, long pointer);

	boolean tryLock(EntityFileAccess<?, ?> entityFile, long pointer, long unit, TimeUnit timeunit);
	
	void unlock(EntityFileAccess<?, ?> entityFile, long pointer);
	
}
