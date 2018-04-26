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
				
				tx.seek(tx.length());
				tx.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, null));
				
				data.seek(data.length());
				data.write(entity);
			}
			else{
				tx.seek(tx.length());
				tx.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, null));
				
				data.seek(data.length());
				data.write(entity);
				
			}
			
			this.lockProvider.lock(this.data, id);
			this.managedRecords.add(id);
			
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
			long id = data.length();
			
			TransactionalEntity<T>[] values = new TransactionalEntity[entity.length];
			long[] ids = new long[entity.length];
			
			for(int i=0;i<values.length;i++){
				ids[i]    = id + i;
				values[i] = new TransactionalEntity<T>(id + i, TransactionalEntity.NEW_RECORD, null); 
			}
			
			tx.seek(tx.length());
			tx.batchWrite(values);
			
			data.seek(data.length());
			data.batchWrite(entity);
			
			for(long i: ids){
				this.lockProvider.lock(this.data, i);
				this.managedRecords.add(i);
			}
			
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
			this.lockProvider.lock(this.data, id);
			this.managedRecords.add(id);
			
			this.data.seek(id);
			R rawData = this.data.readRawEntity();
			
			tx.seek(tx.length());
			tx.writeRawEntity(new RawTransactionEntity<R>(id, TransactionalEntity.UPDATE_RECORD, rawData));
			
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
			int[] indexNotManaged = new int[entity.length];
			int idxNotManaged     = 0;
			
			for(int i=0;i<entity.length;i++){
				
				long r = id[i];
				
				if(!this.managedRecords.contains(r)){
					indexNotManaged[idxNotManaged++] = i;
					this.lockProvider.lock(this.data, r);
					this.managedRecords.add(r);
				}
				
			}
			
			if(idxNotManaged > 0){
				RawTransactionEntity<R>[] values = new RawTransactionEntity[idxNotManaged];
				
				int idx = 0;
				
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
			
			data.seek(data.length());
			data.batchWrite(entity);
			
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
			R rawData = this.data.readRawEntity();

			if(rawData == null){
				return;
			}
			
			tx.seek(tx.length());
			tx.writeRawEntity(new RawTransactionEntity<R>(id, TransactionalEntity.DELETE_RECORD, rawData));
			
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
			
			RawTransactionEntity<R>[] values = new RawTransactionEntity[idxNotManaged];
			
			int idx = 0;
			
			for(int i=0;i<idxNotManaged;i++){
				int x  = indexNotManaged[i];
				long r = id[x];
				
				if(insert){
					tx.seek(tx.length());
					tx.write(new TransactionalEntity<T>(id, TransactionalEntity.NEW_RECORD, null));
				}
				else{
					this.data.seek(r);
					R rawData = this.data.readRawEntity();
					values[idx++] = new RawTransactionEntity<R>(r, TransactionalEntity.UPDATE_RECORD, rawData);
				}
				
				this.data.seek(r);
				R rawData = this.data.readRawEntity();
				values[idx++] = new RawTransactionEntity<R>(r, TransactionalEntity.UPDATE_RECORD, rawData); 
			}
			
			tx.seek(tx.length());
			tx.batchWriteRawEntity(values);
		}
		
	}
	
}
