package org.brandao.entityfilemanager.tx.readcommited;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.tx.EntityFileTransaction;
import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil;
import org.brandao.entityfilemanager.tx.RawTransactionEntity;
import org.brandao.entityfilemanager.tx.TransactionEntityFileAccess;
import org.brandao.entityfilemanager.tx.TransactionException;
import org.brandao.entityfilemanager.tx.TransactionalEntity;
import org.brandao.entityfilemanager.tx.TransactionEntity;

public class ReadCommitedTransactionalEntityFile<T, R> 
	implements TransactionEntity<T, R> {

	private EntityFileAccess<T,R> data;
	
	private TransactionEntityFileAccess<T,R> tx;
	
	private PointerManager<T, R> pointerManager;
	
	private int batchOperationLength;
	
	private Map<Long,Long> pointerMap;
	
	public ReadCommitedTransactionalEntityFile( 
			TransactionEntityFileAccess<T,R> tx, LockProvider lockProvider, long timeout){
		this(tx, lockProvider, timeout, 100);
	}
	
	public ReadCommitedTransactionalEntityFile(TransactionEntityFileAccess<T,R> tx,
			LockProvider lockProvider, long timeout, int batchOperationLength){
		this.data 					= tx.getEntityFileAccess();
		this.tx 					= tx;
		this.batchOperationLength 	= batchOperationLength;
		this.pointerManager 		= new PointerManager<T,R>(tx, lockProvider, timeout);
		this.pointerMap             = new HashMap<Long, Long>();
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
			this.insert(id, entity);
			return id;
		}
		catch(Throwable e){
			throw new EntityFileException(e);
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
			this.insert(id, entity);
			return ids;
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	public void update(long id, T entity) throws EntityFileException{
		
		try{
			this.pointerManager.managerPointer(id, false);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.updateEntity(id, entity, TransactionalEntity.UPDATE_RECORD);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public void update(long[] ids, T[] entities) throws EntityFileException{
		
		try{
			this.pointerManager.managerPointer(ids, false);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			
			int off = 0;
			int q;
			while(off < ids.length){
				
				int nextOff = EntityFileTransactionUtil.getLastSequence(ids, off);
				
				if(nextOff == off){
					this.update(ids[off], entities[off]);
					off++;
				}
				else{
					q               = nextOff - off;
					T[] subEntities = (T[])Array.newInstance(this.data.getType(), q);
					long[] subIds   = new long[q];
					
					System.arraycopy(entities, off, subEntities, 0, q);
					System.arraycopy(ids, off, subIds, 0, q);
					
					this.updateEntity(subIds, subEntities, TransactionalEntity.UPDATE_RECORD);
					
					off = nextOff;
				}
			}
			
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	public void delete(long id) throws EntityFileException{
		
		try{
			this.pointerManager.managerPointer(id, false);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			this.updateEntity(id, (T)null, TransactionalEntity.DELETE_RECORD);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		finally{
			lock.unlock();
		}
	}
	
	public void delete(long[] ids) throws EntityFileException{
		
		try{
			this.pointerManager.managerPointer(ids, false);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			int off = 0;
			int q;
			
			while(off < ids.length){
				
				int nextOff = EntityFileTransactionUtil.getLastSequence(ids, off);
				
				if(nextOff == off){
					this.update(ids[off], null);
					off++;
				}
				else{
					q               = nextOff - off;
					long[] subIds   = new long[q];
					
					System.arraycopy(ids, off, subIds, 0, q);
					
					this.updateEntity(subIds, null, TransactionalEntity.DELETE_RECORD);
					
					off = nextOff;
				}
			}
			
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	public T select(long id){
		return this.select(id, false);
	}
	
	public T select(long id, boolean forUpdate){

		try{
			if(forUpdate){
				this.pointerManager.managerPointer(id, null);
			}
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		
		try{
			return this.selectEntity(id);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		finally{
			lock.unlock();
		}
	}

	public T[] select(long[] id){
		return this.select(id, false);
	}
	
	@SuppressWarnings("unchecked")
	public T[] select(long[] ids, boolean forUpdate){
		
		try{
			if(forUpdate){
				this.pointerManager.managerPointer(ids, null);
			}
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.readLock();
		lock.lock();
		
		try{
			T[] result = (T[]) Array.newInstance(this.data.getType(), ids.length);			
			int off    = 0;
			
			while(off < ids.length){
				int nextOff = EntityFileTransactionUtil.getLastSequence(ids, off);
				this.selectEntity(ids, result, off, nextOff - off);
				off = nextOff;
			}
			
			return result;
		}
		catch(Throwable e){
			throw new EntityFileException(e);
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
			
			long current = 0;
			long max     = this.tx.length();
			
			while(current < max){
				
				this.tx.seek(current);
				
				RawTransactionEntity<R>[] ops = 
					this.tx.batchReadRawEntity(this.batchOperationLength);
				
				RawTransactionEntity<R>[][] map = 
					EntityFileTransactionUtil.mapOperations(ops);
				
				ops = map[TransactionalEntity.NEW_RECORD];
				CommitOperations.insert(ops, data);
				
				ops = map[TransactionalEntity.UPDATE_RECORD];
				CommitOperations.update(ops, data);
				
				ops = map[TransactionalEntity.DELETE_RECORD];
				CommitOperations.delete(ops, data);

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
			
			long current = 0;
			long max     = this.tx.length();
			
			while(current < max){
				
				this.tx.seek(current);
				
				RawTransactionEntity<R>[] ops = 
					this.tx.batchReadRawEntity(this.batchOperationLength);
				
				RawTransactionEntity<R>[][] map = 
					EntityFileTransactionUtil.mapOperations(ops);
				
				ops = map[TransactionalEntity.NEW_RECORD];
				RollbackOperations.insert(ops, data);
				
				ops = map[TransactionalEntity.UPDATE_RECORD];
				RollbackOperations.update(ops, data);
				
				ops = map[TransactionalEntity.DELETE_RECORD];
				RollbackOperations.delete(ops, data);

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
	
	private void insert(long id, T entity) throws IOException{
		
		long txid = this.tx.length();
		
		this.tx.seek(txid);
		this.tx.write(new TransactionalEntity<T>(id, TransactionalEntity.UPDATE_RECORD, entity));
		
		
		this.data.seek(id);
		this.data.write(null);
		
		this.pointerMap.put(id, txid);
	}

	@SuppressWarnings("unchecked")
	private void insert(long id, T[] entities) throws IOException{
		
		TransactionalEntity<T>[] e = new TransactionalEntity[entities.length];
		int max                    = entities.length;
		
		for(int i=0;i<max;i++){
			e[i] = new TransactionalEntity<T>(id + i, TransactionalEntity.UPDATE_RECORD, entities[i]);
		}

		long txid = this.tx.length();
		
		this.tx.seek(txid);
		this.tx.batchWrite(e);
		
		this.data.seek(id);
		this.data.batchWrite(null);
		
		for(int i=0;i<max;i++){
			this.pointerMap.put(id, txid);
		}
		
	}
	
	private void updateEntity(long id, T entity, byte status) throws IOException{
		
		Long pointer = this.pointerMap.get(id);
		
		if(pointer == null){
			long txID = this.tx.length();
			
			this.tx.seek(txID);
			this.tx.write(new TransactionalEntity<T>(id, status, entity));
			
			this.pointerMap.put(id, txID);
		}
		else{
			this.tx.seek(pointer);
			this.tx.write(new TransactionalEntity<T>(id, status, entity));
		}
	}
	
	@SuppressWarnings("unchecked")
	private void updateEntity(long[] ids, T[] entities, byte status) throws IOException{
		
		int[][] opsArray =
				EntityFileTransactionUtil.mapOperations(
						ids, 
						this.pointerMap,
						0,
						entities.length
				);
		
		//não gerenciado
		int[] subIds               = opsArray[TransactionalEntity.NEW_RECORD];
		TransactionalEntity<T>[] e = new TransactionalEntity[subIds.length];
		int max                    = subIds.length;
		long txID                  = this.tx.length();
		long id;
		
		for(int i=0;i<max;i++){
			id = ids[subIds[i]];
			e[i] = new TransactionalEntity<T>(id, status, status == TransactionalEntity.DELETE_RECORD? null : entities[i]);
			this.pointerMap.put(id, txID);
		}
		
		this.tx.seek(txID);
		this.tx.batchWrite(e);

		subIds = opsArray[TransactionalEntity.UPDATE_RECORD];
		max    = subIds.length;
		txID   = this.tx.length();
		TransactionalEntity<T> en;
		
		for(int i=0;i<max;i++){
			id = ids[subIds[i]];
			Long pm = this.pointerMap.get(id);
			en = new TransactionalEntity<T>(id, status, status == TransactionalEntity.DELETE_RECORD? null : entities[i]);
			this.tx.seek(pm);
			this.tx.write(en);
		}
		
	}

	private T selectEntity(long id) throws IOException{
		
		Long pointer = this.pointerMap.get(id);
		
		if(pointer == null){
			this.data.seek(id);
			return this.data.read();
		}
		else{
			this.tx.seek(pointer);
			TransactionalEntity<T> e = this.tx.read();
			return e == null? null : e.getEntity();
		}
	}

	@SuppressWarnings("unchecked")
	private void selectEntity(long[] ids, T[] values, int off, int len) throws IOException{
		
		int[][] opsArray =
				EntityFileTransactionUtil.mapOperations(
						ids, 
						this.pointerMap,
						off,
						len
				);

		int[] subRefIds = opsArray[TransactionalEntity.NEW_RECORD];
		T[] e           = (T[]) Array.newInstance(values.getClass().getComponentType(), subRefIds.length);
		long[] subIds   = EntityFileTransactionUtil.refToId(ids, subRefIds);
		int pos         = 0;
		
		while(pos < subIds.length){
			
			int nextPos = EntityFileTransactionUtil.getLastSequence(ids, pos);
			
			this.data.seek(subIds[pos]);
			
			if(pos == nextPos){
				e[pos] = this.data.read();
			}
			else{
				T[] data = this.data.batchRead(nextPos - pos);
				System.arraycopy(data, 0, e, pos, data.length);
			}
			
			pos = nextPos;
		}
		
		for(int i=0;i<subRefIds.length;i++){
			values[subRefIds[i]] = e[i];
		}

		subRefIds = opsArray[TransactionalEntity.UPDATE_RECORD];
		e         = (T[]) Array.newInstance(values.getClass().getComponentType(), subRefIds.length);
		subIds    = EntityFileTransactionUtil.refToId(ids, subRefIds);
		subIds    = EntityFileTransactionUtil.getTXId(subIds, this.pointerMap);
		pos       = 0;
		
		while(pos < subIds.length){
			
			int nextPos = EntityFileTransactionUtil.getLastSequence(ids, pos);
			
			this.tx.seek(subIds[pos]);
			
			if(pos == nextPos){
				TransactionalEntity<T> r = this.tx.read();
				if(r != null){
					e[pos] = r.getEntity();
				}
			}
			else{
				TransactionalEntity<T>[] rs = this.tx.batchRead(nextPos - pos);
				if(rs != null){
					for(int i=0;i<rs.length;i++){
						TransactionalEntity<T> r = rs[i];
						if(r != null){
							e[pos + i] = r.getEntity();
						}
					}
				}
			}
			
			pos = nextPos;
		}
		
		for(int i=0;i<subRefIds.length;i++){
			values[subRefIds[i]] = e[i];
		}
		
	}

	public byte getTransactionIsolation() throws IOException {
		return EntityFileTransaction.TRANSACTION_READ_COMMITED;
	}
	
}
