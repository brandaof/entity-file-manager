package org.brandao.entityfilemanager;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.brandao.entityfilemanager.tx.EntityFileAccessTransaction;
import org.brandao.entityfilemanager.tx.RawTransactionEntity;
import org.brandao.entityfilemanager.tx.TransactionException;
import org.brandao.entityfilemanager.tx.TransactionalEntity;

public class EntityFileTransaction<T> 
	implements EntityFile<T> {

	private EntityFileAccess<T,byte[]> data;
	
	private EntityFileAccess<Long,byte[]> freeSpace;
	
	private EntityFileAccessTransaction<T,byte[]> tx;
	
	private int batchOperationLength;
	
	public long insert(T entity) throws PersistenceException{
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		long id = -1;
		
		try{
			if(freeSpace.length() == 0){
				id = data.length();
				
				tx.seek(tx.length());
				tx.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, null));
				
				data.seek(data.length());
				data.write(entity);
			}
			else{
				freeSpace.seek(freeSpace.length());
				id = freeSpace.read();
				
				tx.seek(tx.length());
				tx.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, null));
				
				freeSpace.setLength(freeSpace.length() - 1);
				
				data.seek(data.length());
				data.write(entity);
			}
			
			return id;
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	public void update(long id, T entity) throws PersistenceException{
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.data.seek(id);
			byte[] rawData = this.data.readRawEntity();
			
			tx.seek(tx.length());
			tx.writeRawEntity(new RawTransactionEntity<byte[]>(id, TransactionalEntity.UPDATE_RECORD, rawData));
			
			this.data.seek(id);
			this.data.write(entity);
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	public void delete(long id) throws PersistenceException{
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.data.seek(id);
			byte[] rawData = this.data.readRawEntity();

			if(rawData == null){
				return;
			}
			
			tx.seek(tx.length());
			tx.writeRawEntity(new RawTransactionEntity<byte[]>(id, TransactionalEntity.DELETE_RECORD, rawData));
			
			freeSpace.seek(freeSpace.length());
			freeSpace.write(id);
			
			this.data.seek(id);
			this.data.write(null);
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
		finally{
			lock.unlock();
		}
	}
	
	public T select(long id){
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.data.seek(id);
			return data.read();
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
		finally{
			lock.unlock();
		}
	}

	public void begin() throws TransactionException{
		try{
			byte status = this.tx.getTransactionStatus();
			
			if(status != EntityFileAccessTransaction.TRANSACTION_NOT_STARTED){
				throw new TransactionException("transaction has been started");
			}
			
			this.tx.setTransactionStatus(EntityFileAccessTransaction.TRANSACTION_STARTED);
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}
	
	public void commit() throws TransactionException{
		
	}
	
	public void rollback() throws TransactionException{
		try{
			byte status = this.tx.getTransactionStatus();

			if(!this.tx.isStarted()){
				return;
			}
			
			this.tx.setTransactionStatus(EntityFileAccessTransaction.TRANSACTION_STARTED_ROLLBACK);
			
			this.tx.seek(0);
			long current = 0;
			long max     = this.tx.length();
			while(current < max){
				List<RawTransactionEntity<byte[]>> ops = 
						this.tx.batchReadRawEntity(this.batchOperationLength);
				
			}
			
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}

}
