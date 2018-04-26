package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.brandao.entityfilemanager.EntityFile;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
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
	
	public long insert(T entity) throws EntityFileException{
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		long id = this.getNextFreePointer();
		
		try{
			if(id == -1){
				id = data.length();
				data.seek(data.length());
				data.write(entity);
			}
			else{
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
	
	public void update(long[] id, T[] entity) throws EntityFileException{
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
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
		return this.select(id, false);
	}
	
	public T select(long id, boolean notUsed){
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

	public T[] select(long[] id){
		return this.select(id, false);
	}
	
	@SuppressWarnings("unchecked")
	public T[] select(long[] id, boolean notUsed){
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			Arrays.sort(id);
			
			long maxPointer  = this.data.length();
			int i            = 0;
			T[] result       = (T[])Array.newInstance(this.data.getType(), id.length);
			int resultDesloc = 0;
			
			int start;
			int end;
			int count;
			
			while(i < id.length && id[i] < maxPointer){
				start = i;
				count = i;
				end   = i;
				
				while(i < id.length && id[i] < maxPointer){
					
					if(id[i++] != count++){
						end = i + 1;
						break;
					}
					
				}
				
				int qty = end - start;
				
				if(qty == 1){
					this.data.seek(id[start]);
					result[resultDesloc++] = this.data.read();
				}
				else{
					long[] buffIds = new long[qty];
					System.arraycopy(id, start, buffIds, 0, qty);
					resultDesloc += qty;
					
					this.data.seek(buffIds[0]);
					T[] buffEntitys = this.data.batchRead(buffIds);
					System.arraycopy(buffEntitys, 0, result, resultDesloc, buffEntitys.length);
				}
			}
			
			return result;
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
	
}
