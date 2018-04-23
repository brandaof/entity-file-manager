package org.brandao.entityfilemanager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LockProviderImp 
	implements LockProvider{

	private ConcurrentMap<Object, LockObject> entityFileAccessLock;
	
	public LockProviderImp(){
		this.entityFileAccessLock = new ConcurrentHashMap<Object, LockProviderImp.LockObject>();
	}
	
	public void lock(EntityFileAccess<?, ?> entityFile)
			throws InterruptedException {
		this.lockObject(entityFile);
	}

	public boolean tryLock(EntityFileAccess<?, ?> entityFile, 
			long unit, TimeUnit timeunit) throws InterruptedException {
		return this.lockObject(entityFile, unit, timeunit);
	}
	
	public void unlock(EntityFileAccess<?, ?> entityFile)
			throws InterruptedException {
		this.unlockObject(entityFile);
	}

	public void lock(EntityFileAccess<?, ?> entityFile, long pointer)
			throws InterruptedException {
		this.lockObject(new PointerLock(entityFile, pointer));
	}

	public boolean tryLock(EntityFileAccess<?, ?> entityFile, 
			long pointer, long unit, TimeUnit timeunit) throws InterruptedException {
		return this.lockObject(new PointerLock(entityFile, pointer), unit, timeunit);
	}
	
	public void unlock(EntityFileAccess<?, ?> entityFile, long pointer)
			throws InterruptedException {
		this.unlockObject(new PointerLock(entityFile, pointer));
	}
	
	private void lockObject(Object object) throws InterruptedException {
		
		LockObject newLockObject = new LockObject();
		newLockObject.setLocks(new LinkedBlockingQueue<Object>());
		newLockObject.setCurrentLock(object);
		
		Object lock = new Object();
		LockObject lockObject;
		
		synchronized(object){
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
		
		synchronized(object){
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
				Thread.currentThread().wait(maxWaitingTime);
				end = System.currentTimeMillis();
				maxWaitingTime = maxWaitingTime - (end - start);
			}
			
			lockObject.getLocks().remove(lock);
			
			return false;
		}
		
	}
	
	private void unlockObject(Object object) {
		
		synchronized(object){
			LockObject lockObject = 
					this.entityFileAccessLock.get(object);
			
			if(lockObject == null){
				return;
			}
			
			Object next = lockObject.getLocks().poll();
			
			if(next == null){
				this.entityFileAccessLock.remove(object);
			}
			
			lockObject.setCurrentLock(next);
			
			synchronized(next){
				Thread.currentThread().notifyAll();
			}
			
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
		
		private EntityFileAccess<?, ?> entityFile;
		
		private long pointer;

		public PointerLock(EntityFileAccess<?, ?> entityFile, long pointer) {
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
