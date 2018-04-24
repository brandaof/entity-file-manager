package org.brandao.entityfilemanager.tx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;

public class EntityFileTransactionManagerImp 
	implements EntityFileTransactionManager{

	private long txID;
	
	private Lock txIDLock;
	
	private ConcurrentMap<Long, EntityFileTransaction> transactions;
	
	public EntityFileTransactionManagerImp(){
		this.txID = 0;
		this.txIDLock = new ReentrantLock();
		this.transactions = new ConcurrentHashMap<Long, EntityFileTransaction>();
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

	public EntityFileTransaction create(EntityFileManagerConfigurer manager) {
		EntityFileTransactionImp tx = 
			new EntityFileTransactionImp(
				this, 
				new HashMap<EntityFileAccess<?, ?>, TransactionalEntityFile<?, ?>>(), 
				manager, EntityFileTransaction.TRANSACTION_NOT_STARTED, 
				this.getNextTransactionID(), false, false, false);
		
		this.transactions.put(tx.getTransactionID(), tx);
		return tx;
	}

	public EntityFileTransaction load(
			Map<EntityFileAccess<?, ?>, TransactionalEntityFile<?, ?>> transactionFiles,
			EntityFileManagerConfigurer manager, byte status, long transactionID, boolean started,
			boolean rolledBack, boolean commited) {
		return new EntityFileTransactionImp(this, transactionFiles, 
				manager, status, transactionID, started, rolledBack, commited);
	}

	public void close(EntityFileTransaction transaction) throws TransactionException {
		
		EntityFileTransactionImp tx = (EntityFileTransactionImp)transaction;
		
		if(tx.isCommited() || tx.isRolledBack() || tx.isClosed() || !tx.isStarted()){
			return;
		}
		
		tx.rollback();
		tx.setClosed(true);
		
		this.transactions.remove(tx.getTransactionID());
	}

}
