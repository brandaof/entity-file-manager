package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;
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
	
	private PointerManager<T, R> pointerManager;
	
	private int batchOperationLength;
	
	public TransactionalEntityFile(EntityFileAccess<T,R> data, 
			TransactionEntityFileAccess<T,R> tx, PointerManager<T, R> pointerManager){
		this(data,tx, pointerManager, 100);
	}
	
	public TransactionalEntityFile(EntityFileAccess<T,R> data, 
			TransactionEntityFileAccess<T,R> tx, PointerManager<T, R> pointerManager, 
			int batchOperationLength){
		this.data 					= data;
		this.tx 					= tx;
		this.batchOperationLength 	= batchOperationLength;
		this.pointerManager 		= pointerManager;
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
		
		try{
			long id = this.getNextFreePointer(false);
			this.pointerManager.managerPointer(id, true);
			this.write(id, entity);
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
			long id    = this.getNextFreePointer(true);
			long[] ids = new long[entity.length];
			
			for(int i=0;i<ids.length;i++){
				ids[i] = id + i;
			}
			
			this.pointerManager.managerPointer(ids, true);
			this.write(id, entity);
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
			this.pointerManager.managerPointer(id, false);
			this.write(id, entity);
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public void update(long[] ids, T[] entities) throws EntityFileException{
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.pointerManager.managerPointer(ids, false);
			Map<Long,Integer> mappedIndex = EntityFileTransactionUtil.getMappedIdIndex(ids);
			
			int off = 0;
			
			while(off < ids.length){
				
				long[] group =                                                            
					EntityFileTransactionUtil.getNextSequenceGroup(ids, off);
				
				if(group == null){
					this.write(ids[off], entities[off]);
					off++;
				}
				else{
					T[] values = (T[])Array.newInstance(this.data.getType(), group.length);
					
					for(int i=0;i<group.length;i++){
						int idx = mappedIndex.get(group[i]);
						values[i] = entities[idx];
					}
					
					this.write(group[0], values);
					
					off += group.length;
				}
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
			this.pointerManager.managerPointer(id, true);
			this.write(id, (T)null);
		}
		catch(Throwable e){
			throw new PersistenceException(e);
		}
		finally{
			lock.unlock();
		}
	}
	
	public void delete(long[] ids) throws EntityFileException{
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.pointerManager.managerPointer(ids, false);
			
			int off = 0;
			
			while(off < ids.length){
				
				long[] group =                                                            
					EntityFileTransactionUtil.getNextSequenceGroup(ids, off);
				
				if(group == null){
					this.write(ids[off], (T)null);
					off++;
				}
				else{
					this.write(group[0], (T[])null);
					off += group.length;
				}
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
	
	public T select(long id, boolean forUpdate){
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			if(forUpdate){
				this.pointerManager.managerPointer(id, null);
			}
			
			return this.read(id);
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
	public T[] select(long[] id, boolean forUpdate){
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			if(forUpdate){
				this.pointerManager.managerPointer(id, null);
			}
			
			Arrays.sort(id);
			int off    = 0;
			T[] result = (T[])Array.newInstance(this.data.getType(), id.length);
			
			while(off < id.length){
				long[] group = EntityFileTransactionUtil.getNextSequenceGroup(id, off);
				
				if(group == null){
					result[off++] = this.read(id[off]);
				}
				else{
					T[] buffEntitys = this.read(group[0], group.length);
					System.arraycopy(buffEntitys, 0, result, off, buffEntitys.length);
					off += group.length;
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

	public long getNextFreePointer(boolean batch) throws IOException{
		return this.data.length();
	}
	
	/* private */
	
	private void write(long id, T entity) throws IOException{
		data.seek(id);
		data.write(entity);
	}

	private void write(long id, T[] entities) throws IOException{
		data.seek(id);
		data.batchWrite(entities);
	}

	private T read(long id) throws IOException{
		data.seek(id);
		return data.read();
	}

	private T[] read(long id, int len) throws IOException{
		data.seek(id);
		return data.batchRead(len);
	}
	
}
