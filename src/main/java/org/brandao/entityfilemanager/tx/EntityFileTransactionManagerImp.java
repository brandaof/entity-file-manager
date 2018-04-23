package org.brandao.entityfilemanager.tx;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EntityFileTransactionManagerImp 
	implements EntityFileTransactionManager{

	private long txID;
	
	private Lock txIDLock;
	
	public EntityFileTransactionManagerImp(){
		this.txID = 0;
		this.txIDLock = new ReentrantLock();
	}
	
	public long getNextTransactionID() {
		this.txIDLock.lock();
		try{
			long current = this.txID++;
			return current;
		}
		finally{
			this.txIDLock.unlock();
		}
	}

	public EntityFileTransaction begin() {
		return null;
	}

	public EntityFileTransaction create() {
		return null;
	}

	public void close(EntityFileTransaction tx) {
		// TODO Auto-generated method stub
		
	}

}
