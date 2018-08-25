package org.brandao.entityfilemanager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LockProviderImp 
	implements LockProvider{

	private static final long serialVersionUID = 7756665738231339801L;
	
	private ConcurrentMap<Object, LockObject> entityFileAccessLock;
	
	public LockProviderImp(){
		this.entityFileAccessLock = new ConcurrentHashMap<Object, LockProviderImp.LockObject>();
	}
	
	public void lock(EntityFileAccess<?, ?, ?> entityFile)
			throws LockException {
		try{
			this.lockObject(entityFile);
		}
		catch(Throwable e){
			throw new LockException(e);
		}
	}

	public boolean tryLock(EntityFileAccess<?, ?, ?> entityFile, 
			long unit, TimeUnit timeunit) throws LockException {
		try{
			return this.lockObject(entityFile, unit, timeunit);
		}
		catch(Throwable e){
			throw new LockException(e);
		}
	}
	
	public void unlock(EntityFileAccess<?, ?, ?> entityFile)
			throws LockException {
		try{
			this.unlockObject(entityFile);
		}
		catch(Throwable e){
			throw new LockException(e);
		}
	}

	public void lock(EntityFileAccess<?, ?, ?> entityFile, long pointer)
			throws LockException {
		try{
			this.lockObject(new PointerLock(entityFile, pointer));
		}
		catch(Throwable e){
			throw new LockException(e);
		}
	}

	public boolean tryLock(EntityFileAccess<?, ?, ?> entityFile, 
			long pointer, long unit, TimeUnit timeunit) throws LockException {
		try{
			return this.lockObject(new PointerLock(entityFile, pointer), unit, timeunit);
		}
		catch(Throwable e){
			throw new LockException(e);
		}
	}
	
	public void unlock(EntityFileAccess<?, ?, ?> entityFile, long pointer)
			throws LockException {
		try{
			this.unlockObject(new PointerLock(entityFile, pointer));
		}
		catch(Throwable e){
			throw new LockException(e);
		}
	}
	
	private void lockObject(Object object) throws InterruptedException {
		
		LockObject newLockObject = new LockObject();
		newLockObject.setLocks(new LinkedBlockingQueue<Object>());
		newLockObject.setCurrentLock(object);

		Object lock = new Object();
		LockObject lockObject;
		
		synchronized(this) {
			lockObject = 
					this.entityFileAccessLock.putIfAbsent(
							object, 
							newLockObject
					);
			
			if(lockObject == null){
				return;
			}
			
			lockObject.getLocks().put(lock);
		}
		
		synchronized(lock){
			
			while(true){
				Thread.currentThread().wait();
				
				if(lockObject.getCurrentLock() == lock){
					return;
				}
				
			}
		}
		
	}

	private boolean lockObject(Object object, 
			long unit, TimeUnit timeunit) throws InterruptedException {
		
		LockObject newLockObject = new LockObject();
		newLockObject.setLocks(new LinkedBlockingQueue<Object>());
		newLockObject.setCurrentLock(object);
		
		Object lock = new Object();
		LockObject lockObject;
		
		synchronized(this) {
			lockObject = 
					this.entityFileAccessLock.putIfAbsent(
							object, 
							newLockObject
					);
			
			if(lockObject == null){
				return true;
			}
			
			lockObject.getLocks().put(lock);
		}
		
		long maxWaitingTime = timeunit.toMillis(unit);
		
		synchronized(lock){
			
			long start;
			long end;
			
			while(maxWaitingTime > 0){
				
				if(lockObject.getCurrentLock() == lock){
					return true;
				}
				
				start = System.currentTimeMillis();
				lock.wait(maxWaitingTime);
				end = System.currentTimeMillis();
				maxWaitingTime = maxWaitingTime - (end - start);
			}
			
			return lockObject.getLocks().remove(lock);
		}
		
	}
	
	private void unlockObject(Object object) {
		
		Object next;
		
		synchronized(this) {
			LockObject lockObject = 
					this.entityFileAccessLock.get(object);
			
			if(lockObject == null){
				return;
			}
			
			next = lockObject.getLocks().poll();
			
			if(next == null){
				this.entityFileAccessLock.remove(object, lockObject);
				return;
			}
			
			lockObject.setCurrentLock(next);
		}
		
		synchronized(next){
			next.notifyAll();
		}
		
	}

	private static class LockObject{
		
		private Object currentLock;
		
		private BlockingQueue<Object> locks;

		public Object getCurrentLock() {
			return currentLock;
		}

		public void setCurrentLock(Object currentLock) {
			this.currentLock = currentLock;
		}

		public BlockingQueue<Object> getLocks() {
			return locks;
		}

		public void setLocks(BlockingQueue<Object> locks) {
			this.locks = locks;
		}
		
	}

	private static class PointerLock {
		
		private EntityFileAccess<?, ?, ?> entityFile;
		
		private long pointer;

		public PointerLock(EntityFileAccess<?, ?, ?> entityFile, long pointer) {
			this.entityFile = entityFile;
			this.pointer = pointer;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((entityFile == null) ? 0 : entityFile.hashCode());
			result = prime * result + (int) (pointer ^ (pointer >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PointerLock other = (PointerLock) obj;
			if (entityFile == null) {
				if (other.entityFile != null)
					return false;
			} else if (!entityFile.equals(other.entityFile))
				return false;
			if (pointer != other.pointer)
				return false;
			return true;
		}
		
		
	}
}
