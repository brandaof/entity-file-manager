package org.brandao.entityfilemanager.tx;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;

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

	public EntityFileTransaction create() {
		return null;
	}

	public EntityFileTransaction load(
			Map<EntityFileAccess<?, ?>, TransactionalEntityFile<?, ?>> transactionFiles,
			EntityFileManagerConfigurer manager, byte status, long transactionID, boolean started,
			boolean rolledBack, boolean commited) {
		return new EntityFileTransactionImp(this, transactionFiles, 
				manager, status, transactionID, started, rolledBack, commited);
	}

	public void close(EntityFileTransaction tx) throws TransactionException {
		if(!tx.isCommited() && !tx.isRolledBack()){
			tx.rollback();
		}
	}

}
