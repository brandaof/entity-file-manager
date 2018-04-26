package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.brandao.entityfilemanager.EntityFile;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.PersistenceException;

public class TransactionalEntityFile<T, R> 
	implements EntityFile<T> {

	private EntityFileAccess<T,R> data;
	
	private TransactionEntityFileAccess<T,R> tx;
	
	private int batchOperationLength;
	
	private Set<Long> managedRecords;
	
	private LockProvider lockProvider;
	
	private long timeout;
	
	public TransactionalEntityFile(EntityFileAccess<T,R> data, 
			TransactionEntityFileAccess<T,R> tx){
		this(data,tx, 100);
	}
	
	public TransactionalEntityFile(EntityFileAccess<T,R> data, 
			TransactionEntityFileAccess<T,R> tx, int batchOperationLength){
		this.data = data;
		this.tx = tx;
		this.batchOperationLength = batchOperationLength;
	}
	
	public void setTransactionStatus(byte value) throws IOException{
		this.tx.setTransactionStatus(value);
	}
	
	public byte getTransactionStatus() throws IOException{
		return this.tx.getTransactionStatus();
	}
	
	public long insert(T entity) throws EntityFileException{
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		long id = this.getNextFreePointer();
		
		try{
			if(id == -1){
				id = data.length();
				
				this.managerPointer(id, true);
				
				data.seek(data.length());
				data.write(entity);
			}
			else{
				this.managerPointer(id, true);
				
				data.seek(id);
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
	
	@SuppressWarnings("unchecked")
	public long[] insert(T[] entity) throws EntityFileException{
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			long id    = data.length();
			long[] ids = new long[entity.length];
			
			for(int i=0;i<ids.length;i++){
				ids[i] = id + i;
			}
			
			this.managerPointer(ids, true);
			
			data.seek(id);
			data.batchWrite(entity);
			
			return ids;
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
			this.managerPointer(id, false);
			
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
	
	@SuppressWarnings("unchecked")
	public void update(long[] id, T[] entity) throws EntityFileException{
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.managerPointer(id, false);
			
			for(int i=0;i<id.length;i++){
				data.seek(id[i]);
				data.write(entity[i]);
			}
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
			this.managerPointer(id, false);
			
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
	
	public void delete(long[] id) throws EntityFileException{
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.managerPointer(id, false);
			
			for(int i=0;i<id.length;i++){
				data.seek(id[i]);
				data.write(null);
			}
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
			
			if(status != EntityFileTransaction.TRANSACTION_NOT_STARTED){
				throw new TransactionException("transaction has been started");
			}
			
			this.tx.setTransactionStatus(EntityFileTransaction.TRANSACTION_STARTED);
		}
		catch(TransactionException e){
			throw e;
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}
	
	public void commit() throws TransactionException{
		try{
			if(!this.tx.isStarted()){
				return;
			}
			
			this.tx.seek(0);
			
			long current = 0;
			long max     = this.tx.length();
			
			while(current < max){
				
				RawTransactionEntity<R>[] ops = 
					this.tx.batchReadRawEntity(this.batchOperationLength);
				
				RawTransactionEntity<R>[][] map = 
					EntityFileTransactionUtil.mapOperations(ops);
				
				ops = map[TransactionalEntity.NEW_RECORD];
				RollbackOperations.insert(ops, data);
				
				ops = map[TransactionalEntity.UPDATE_RECORD];
				RollbackOperations.insert(ops, data);
				
				ops = map[TransactionalEntity.DELETE_RECORD];
				RollbackOperations.insert(ops, data);

				current += ops.length;
			}
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}		
	}
	
	public void rollback() throws TransactionException{
		try{
			if(!this.tx.isStarted()){
				return;
			}
			
			this.tx.seek(0);
			
			long current = 0;
			long max     = this.tx.length();
			
			while(current < max){
				
				RawTransactionEntity<R>[] ops = 
					this.tx.batchReadRawEntity(this.batchOperationLength);
				
				RawTransactionEntity<R>[][] map = 
					EntityFileTransactionUtil.mapOperations(ops);
				
				ops = map[TransactionalEntity.NEW_RECORD];
				RollbackOperations.insert(ops, data);
				
				ops = map[TransactionalEntity.UPDATE_RECORD];
				RollbackOperations.insert(ops, data);
				
				ops = map[TransactionalEntity.DELETE_RECORD];
				RollbackOperations.insert(ops, data);

				current += ops.length;
			}
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
	}

	private long getNextFreePointer(){
		return -1;
	}

	private void managerPointer(long id, boolean insert) throws IOException{
		if(!this.managedRecords.contains(id)){
			this.lockProvider.lock(this.data, id);
			this.managedRecords.add(id);
			
			if(insert){
				tx.seek(tx.length());
				tx.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, null));
			}
			else{
				this.data.seek(id);
				R rawData = this.data.readRawEntity();
				tx.seek(tx.length());
				tx.writeRawEntity(new RawTransactionEntity<R>(id, TransactionalEntity.UPDATE_RECORD, rawData));
			}
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void managerPointer(long[] id, boolean insert) throws IOException{
		
		int[] indexNotManaged = new int[id.length];
		int idxNotManaged     = 0;
		
		for(int i=0;i<id.length;i++){
			
			long r = id[i];
			
			if(!this.managedRecords.contains(r)){
				indexNotManaged[idxNotManaged++] = i;
				this.lockProvider.lock(this.data, r);
				this.managedRecords.add(r);
			}
			
		}

		if(idxNotManaged > 0){
			
			int idx = 0;
			
			if(insert){
				TransactionalEntity<T>[] values = new TransactionalEntity[idxNotManaged];
				
				for(int i=0;i<idxNotManaged;i++){
					int x  = indexNotManaged[i];
					long r = id[x];
					
					values[idx++] = new TransactionalEntity<T>(r, TransactionalEntity.NEW_RECORD, null);
					
				}
				
				tx.seek(tx.length());
				tx.batchWrite(values);
				
			}
			else{
				RawTransactionEntity<R>[] values = new RawTransactionEntity[idxNotManaged];
				
				for(int i=0;i<idxNotManaged;i++){
					int x  = indexNotManaged[i];
					long r = id[x];
					
					this.data.seek(r);
					R rawData = this.data.readRawEntity();
					
					values[idx++] = new RawTransactionEntity<R>(r, TransactionalEntity.UPDATE_RECORD, rawData);
					
				}
				
				tx.seek(tx.length());
				tx.batchWriteRawEntity(values);
				
			}

		}
		
	}
	
}
