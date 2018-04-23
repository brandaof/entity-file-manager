package org.brandao.entityfilemanager;

import java.util.concurrent.TimeUnit;

public interface LockProvider {

	void lock(EntityFileAccess<?, ?> entityFile) throws InterruptedException ;

	boolean tryLock(EntityFileAccess<?, ?> entityFile, long unit, TimeUnit timeunit) throws InterruptedException ;
	
	void unlock(EntityFileAccess<?, ?> entityFile) throws InterruptedException ;
	
	void lock(EntityFileAccess<?, ?> entityFile, long pointer) throws InterruptedException ;

	boolean tryLock(EntityFileAccess<?, ?> entityFile, long pointer, long unit, TimeUnit timeunit) throws InterruptedException ;
	
	void unlock(EntityFileAccess<?, ?> entityFile, long pointer) throws InterruptedException ;
	
}
