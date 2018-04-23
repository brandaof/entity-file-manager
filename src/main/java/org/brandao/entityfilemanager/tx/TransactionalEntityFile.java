package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.brandao.entityfilemanager.EntityFile;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.PersistenceException;

public class TransactionalEntityFile<T, R> 
	implements EntityFile<T> {

	private EntityFileAccess<T,R> data;
	
	private TransactionEntityFileAccess<T,R> tx;
	
	private int batchOperationLength;
	
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
	
	public long insert(T entity) throws PersistenceException{
		
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
			
			if(status != TransactionEntityFileAccess.TRANSACTION_NOT_STARTED){
				throw new TransactionException("transaction has been started");
			}
			
			this.tx.setTransactionStatus(TransactionEntityFileAccess.TRANSACTION_STARTED);
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
	
}
