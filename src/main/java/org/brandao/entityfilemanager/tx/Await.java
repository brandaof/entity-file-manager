package org.brandao.entityfilemanager.tx;

@Deprecated
public class Await {

	private int count;
	
	public void await(){
		synchronized(this){
			count++;
		}
	}

	public void release(){
		synchronized(this){
			count--;
			if(count == 0){
				this.notify();
			}
		}
	}
	
	public void waitAll() throws InterruptedException{
		synchronized(this){
			this.wait();
		}
	}
	
}
