package org.brandao.entityfilemanager.tx.readcommited;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

public class ReadCommitedTransactionalEntityFile<T, R, H> 
	implements TransactionEntity<T, R> {

	private EntityFileAccess<T,R,H> data;
	
	private TransactionEntityFileAccess<T,R,H> tx;
	
	private PointerManager<T, R, H> pointerManager;
	
	private int batchOperationLength;
	
	private Map<Long,Long> pointerMap;
	
	public ReadCommitedTransactionalEntityFile( 
			TransactionEntityFileAccess<T,R,H> tx, LockProvider lockProvider, long timeout){
		this(tx, lockProvider, timeout, 100);
	}
	
	public ReadCommitedTransactionalEntityFile(TransactionEntityFileAccess<T,R,H> tx,
			LockProvider lockProvider, long timeout, int batchOperationLength){
		this.data 					= tx.getEntityFileAccess();
		this.tx 					= tx;
		this.batchOperationLength 	= batchOperationLength;
		this.pointerManager 		= new PointerManager<T,R,H>(tx, lockProvider, timeout);
		this.pointerMap             = new HashMap<Long, Long>();
	}
	
	public void setTransactionStatus(byte value) throws IOException{
		this.tx.setTransactionStatus(value);
	}
	
	public byte getTransactionStatus() throws IOException{
		return this.tx.getTransactionStatus();
	}

	public long insert(T entity) throws EntityFileException{
		
		try{
			return this.insertEntity(entity);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
	}

	public long[] insert(T[] entity) throws EntityFileException{
		
		try{
			return this.insertEntity(entity);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
	}
	
	public void update(long id, T entity) throws EntityFileException{
		
		try{
			this.pointerManager.managerPointer(id);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		try{
			this.updateEntity(id, entity, TransactionalEntity.UPDATE_RECORD);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public void update(long[] ids, T[] entities) throws EntityFileException{
		
		try{
			this.pointerManager.managerPointer(ids);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		try{
			
			int off = 0;
			int q;
			while(off < ids.length){
				
				int nextOff = EntityFileTransactionUtil.getLastSequence(ids, off);
				q           = nextOff - off;
				
				if(q == 1){
					this.update(ids[off], entities[off]);
					off++;
				}
				else{
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
		
	}
	
	public void delete(long id) throws EntityFileException{
		
		try{
			this.pointerManager.managerPointer(id);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		try{
			this.updateEntity(id, (T)null, TransactionalEntity.DELETE_RECORD);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
	}
	
	public void delete(long[] ids) throws EntityFileException{
		
		try{
			this.pointerManager.managerPointer(ids);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
		try{
			int off = 0;
			int q;
			
			while(off < ids.length){
				
				int nextOff = EntityFileTransactionUtil.getLastSequence(ids, off);
				q           = nextOff - off;
				
				if(q == 1){
					this.update(ids[off], null);
					off++;
				}
				else{
					long[] subIds = new long[q];
					
					System.arraycopy(ids, off, subIds, 0, q);
					
					this.updateEntity(subIds, null, TransactionalEntity.DELETE_RECORD);
					
					off = nextOff;
				}
			}
			
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		
	}
	
	public T select(long id){
		return this.select(id, false);
	}
	
	public T select(long id, boolean forUpdate){

		try{
			if(forUpdate){
				this.pointerManager.managerPointer(id);
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
				this.pointerManager.managerPointer(ids);
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
					this.tx.batchReadRaw(this.batchOperationLength);

				current += ops.length;
				
				RawTransactionEntity<R>[][] map = 
					EntityFileTransactionUtil.mapOperations(ops);
				
				ops = map[TransactionalEntity.NEW_RECORD];
				CommitOperations.insert(ops, data);
				
				ops = map[TransactionalEntity.UPDATE_RECORD];
				CommitOperations.update(ops, data);
				
				ops = map[TransactionalEntity.DELETE_RECORD];
				CommitOperations.delete(ops, data);

			}
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		finally{
			this.pointerManager.release();
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
					this.tx.batchReadRaw(this.batchOperationLength);

				current += ops.length;
				
				RawTransactionEntity<R>[][] map = 
					EntityFileTransactionUtil.mapOperations(ops);
				
				ops = map[TransactionalEntity.NEW_RECORD];
				RollbackOperations.insert(ops, data);
				
				ops = map[TransactionalEntity.UPDATE_RECORD];
				RollbackOperations.update(ops, data);
				
				ops = map[TransactionalEntity.DELETE_RECORD];
				RollbackOperations.delete(ops, data);

			}
		}
		catch(Throwable e){
			throw new TransactionException(e);
		}
		finally{
			this.pointerManager.release();
		}
		
	}

	public long getNextFreePointer(boolean batch) throws IOException{
		return this.data.length();
	}
	
	/* private */
	
	private long insertEntity(T entity) throws IOException{

		long id;
		long txid;
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		try{
			id = this.getNextFreePointer(false);
			
			this.data.seek(id);
			this.data.write(null);
			
			this.pointerManager.managerPointer(id);
			
		}
		finally{
			lock.unlock();
		}
		
		
		readWritelock = tx.getLock();
		lock = readWritelock.writeLock();
		lock.lock();
		try{
			txid = this.tx.length();
			this.tx.seek(txid);
			this.tx.write(new TransactionalEntity<T>(id, TransactionalEntity.UPDATE_RECORD, entity));
		}
		finally{
			lock.unlock();
		}
		
		this.pointerMap.put(id, txid);
		return id;
	}

	@SuppressWarnings("unchecked")
	private long[] insertEntity(T[] entities) throws IOException{
		
		long id;
		long txid;
		int max = entities.length;
		
		ReadWriteLock readWritelock = data.getLock();
		Lock lock = readWritelock.writeLock();
		lock.lock();
		try{
			id = this.getNextFreePointer(false);
			
			this.data.seek(id);
			this.data.batchWrite((T[])Array.newInstance(entities.getClass().getComponentType(), max));
			
			for(int i=0;i<max;i++){
				this.pointerManager.managerPointer(id + i);
			}

		}
		finally{
			lock.unlock();
		}
		
		long eid;
		long etxid;
		long[] ids = new long[max];
		TransactionalEntity<T>[] e = new TransactionalEntity[max];
		
		readWritelock = tx.getLock();
		lock = readWritelock.writeLock();
		lock.lock();
		try{
			txid = this.tx.length();
			
			for(int i=0;i<max;i++){
				eid   = id + i;
				etxid = txid + i;
				
				ids[i] = eid;
				e[i]   = new TransactionalEntity<T>(eid, TransactionalEntity.UPDATE_RECORD, entities[i]);
				
				this.pointerMap.put(eid, etxid);
			}
			
			this.tx.seek(txid);
			this.tx.batchWrite(e);
		}
		finally{
			lock.unlock();
		}
		
		return ids;		
	}
	
	private void updateEntity(long id, T entity, byte status) throws IOException{
		
		if(id >= this.data.length()){
			throw new IOException("entity not found: " + id);
		}
		
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
			this.pointerMap.put(id, txID + i);
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
		int q;
		while(pos < subIds.length){
			
			int nextPos = EntityFileTransactionUtil.getLastSequence(ids, pos);
			q = nextPos - pos;
			
			this.data.seek(subIds[pos]);
			
			if(q == 1){
				e[pos] = this.data.read();
			}
			else{
				T[] data = this.data.batchRead(q);
				System.arraycopy(data, 0, e, pos, q);
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
			q = nextPos - pos;
			
			this.tx.seek(subIds[pos]);
			
			if(q == 1){
				TransactionalEntity<T> r = this.tx.read();
				if(r != null){
					e[pos] = r.getEntity();
				}
			}
			else{
				TransactionalEntity<T>[] rs = this.tx.batchRead(q);
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

	public void close() throws IOException {
		this.tx.close();
	}

	public void delete() throws IOException {
		this.tx.delete();
	}

	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeObject(data);
		stream.writeObject(tx);
		stream.writeObject(pointerManager);
		stream.writeInt(batchOperationLength);
		stream.writeObject(pointerMap);
    }

    @SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    	data                 = (EntityFileAccess<T, R, H>) stream.readObject();
    	tx                   = (TransactionEntityFileAccess<T, R, H>) stream.readObject();
    	pointerManager       = (PointerManager<T, R, H>) stream.readObject();
    	batchOperationLength = stream.readInt();
    	pointerMap           = (Map<Long, Long>) stream.readObject();
    }
	
}
