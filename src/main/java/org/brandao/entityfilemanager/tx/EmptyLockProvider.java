package org.brandao.entityfilemanager.tx;

import java.util.concurrent.TimeUnit;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.LockException;
import org.brandao.entityfilemanager.LockProvider;

public class EmptyLockProvider 
	implements LockProvider{

	private static final long serialVersionUID = -9204827088417084784L;

	public void lock(EntityFileAccess<?, ?, ?> entityFile) throws LockException {
	}

	public boolean tryLock(EntityFileAccess<?, ?, ?> entityFile, long unit,
			TimeUnit timeunit) throws LockException {
		return false;
	}

	public void unlock(EntityFileAccess<?, ?, ?> entityFile)
			throws LockException {
	}

	public void lock(EntityFileAccess<?, ?, ?> entityFile, long pointer)
			throws LockException {
	}

	public boolean tryLock(EntityFileAccess<?, ?, ?> entityFile, long pointer,
			long unit, TimeUnit timeunit) throws LockException {
		return false;
	}

	public void unlock(EntityFileAccess<?, ?, ?> entityFile, long pointer)
			throws LockException {
	}

}
