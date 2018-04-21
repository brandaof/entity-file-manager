package org.brandao.entityfilemanager;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.brandao.entityfilemanager.tx.EntityFileAccessTransaction;
import org.brandao.entityfilemanager.tx.TransactionalEntity;

public class EntityFileTransaction<T> {

	private EntityFileAccess<T> data;
	
	private EntityFileAccess<Long> freeSpace;
	
	public long insert(T entity, EntityFileAccessTransaction<T> tx) throws PersistenceException{
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		long id = -1;
		
		try{
			if(freeSpace.length() == 0){
				id = data.length();
				
				tx.seek(tx.length());
				tx.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, entity));
				
				data.seek(data.length());
				data.write(null);
			}
			else{
				freeSpace.seek(freeSpace.length());
				id = freeSpace.read();
				
				tx.seek(tx.length());
				tx.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, entity));
				
				freeSpace.setLength(freeSpace.length() - 1);
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
	
	public void update(long id, T entity, EntityFileAccess<T> tx) throws PersistenceException{
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			tx.seek(tx.length());
			tx.write(new TransactionalEntity<T>(id, TransactionalEntity.UPDATE_RECORD, entity));
			
			data.seek(data.length());
			data.write(null);
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	public void delete(long id, EntityFileAccess<T> entityFileaccess) throws PersistenceException{
		
	}
	
	public T select(long id, EntityFileAccess<T> entityFileaccess){
		
	}

}
