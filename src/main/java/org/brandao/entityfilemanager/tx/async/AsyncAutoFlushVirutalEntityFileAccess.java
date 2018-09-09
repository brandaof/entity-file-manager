package org.brandao.entityfilemanager.tx.async;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.brandao.entityfilemanager.EntityFileAccess;

public class AsyncAutoFlushVirutalEntityFileAccess<T, R, H> 
	extends AutoFlushVirutalEntityFileAccess<T, R, H>{

	private CountDownLatch countDownLatch;
	
	private Lock resyncLock;
	
	public AsyncAutoFlushVirutalEntityFileAccess(
			EntityFileAccess<T, R, H> e) throws IOException {
		super(e);
		this.countDownLatch = new CountDownLatch(0);
		this.resyncLock     = new ReentrantLock();
	}
	
	public void register(){
		resyncLock.lock();
		try{
			this.countDownLatch.countUp();
		}
		finally{
			resyncLock.unlock();
		}
	}

	public void unregister(){
		resyncLock.lock();
		try{
			this.countDownLatch.countDown();
		}
		finally{
			resyncLock.unlock();
		}
	}
	
	public void resync() throws IOException{
		resyncLock.lock();
		try{
			this.countDownLatch.await();
			super.resync();
		}
		catch(Throwable e){
			throw new IOException("resync fail", e);
		}
		finally{
			resyncLock.unlock();
		}
		
	}

	public void tryResync() throws IOException{
		resyncLock.lock();
		try{
			if(countDownLatch.getCount() == 0){
				super.resync();
			}
		}
		catch(Throwable e){
			throw new IOException("resync fail", e);
		}
		finally{
			resyncLock.unlock();
		}
	}
	

}
