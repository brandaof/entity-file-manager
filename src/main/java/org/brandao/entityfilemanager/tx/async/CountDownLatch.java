package org.brandao.entityfilemanager.tx.async;

/**
 * Implementação básica do CountDownLatch.
 * 
 * @author Brandao
 *
 */
public class CountDownLatch {

	private volatile long count;
	
	public CountDownLatch(long value){
		this.count = value;
	}
	
	public long getCount(){
		return count;
	}
	public void countUp(){
		synchronized(this){
			++count;
		}
	}
	
	public void countDown(){
		synchronized(this){
			--count;
			if(count < 0){
				throw new IllegalStateException();
			}
			this.notify();
		}
	}
	
	public void await() throws InterruptedException{
		synchronized(this){
			while(true){
				if(count == 0){
					break;
				}
				this.wait();
			}
		}
	}
	
}
