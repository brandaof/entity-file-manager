package org.brandao.entityfilemanager.tx.readcommited;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

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
			return this.selectEntity(id);
		}
		catch(Throwable e){
			throw new EntityFileException(e);
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
				
				tx.seek(current);
				
				RawTransactionEntity<R>[] ops = 
					tx.batchReadRaw(this.batchOperationLength);

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
			
			tx.flush();
			data.flush();
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
			long max     = tx.length();
			
			while(current < max){
				
				tx.seek(current);
				
				RawTransactionEntity<R>[] ops = 
					tx.batchReadRaw(this.batchOperationLength);

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
			
			tx.flush();
			data.flush();
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
		
		Lock lock = data.getLock();
		lock.lock();
		try{
			id = this.getNextFreePointer(false);
			
			data.seek(id);
			this.data.write(null);
			
			this.pointerManager.managerPointer(id);
		}
		finally{
			lock.unlock();
		}
		
		
		lock = tx.getLock();
		lock.lock();
		try{
			txid = tx.length();
			tx.seek(txid);
			tx.write(new TransactionalEntity<T>(id, TransactionalEntity.UPDATE_RECORD, entity));
			tx.flush();
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
		
		Lock lock = data.getLock();
		lock.lock();
		try{
			id = getNextFreePointer(false);
			
			data.seek(id);
			this.data.batchWrite((T[])Array.newInstance(entities.getClass().getComponentType(), max));
			data.flush();
			
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
		
		lock = tx.getLock();
		lock.lock();
		try{
			txid = tx.length();
			
			for(int i=0;i<max;i++){
				eid   = id + i;
				etxid = txid + i;
				
				ids[i] = eid;
				e[i]   = new TransactionalEntity<T>(eid, TransactionalEntity.UPDATE_RECORD, entities[i]);
				
				pointerMap.put(eid, etxid);
			}
			
			tx.seek(txid);
			tx.batchWrite(e);
			tx.flush();
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
			long txID;
			
			Lock lock = tx.getLock();
			lock.lock();
			try{
				txID = this.tx.length();
				
				tx.seek(txID);
				tx.write(new TransactionalEntity<T>(id, status, entity));
				tx.flush();
			}
			finally{
				lock.unlock();
			}
			
			this.pointerMap.put(id, txID);
		}
		else{
			Lock lock = tx.getLock();
			lock.lock();
			try{
				tx.seek(pointer);
				tx.write(new TransactionalEntity<T>(id, status, entity));
				tx.flush();
			}
			finally{
				lock.unlock();
			}
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
		long id;
		
		Lock lock = tx.getLock();
		lock.lock();
		try{
			
			long txID = tx.length();
			
			for(int i=0;i<max;i++){
				id   = ids[subIds[i]];
				e[i] = new TransactionalEntity<T>(id, status, status == TransactionalEntity.DELETE_RECORD? null : entities[i]);
				pointerMap.put(id, txID + i);
			}
			
			tx.seek(txID);
			tx.batchWrite(e);
			tx.flush();
		}
		finally{
			lock.unlock();
		}

		subIds = opsArray[TransactionalEntity.UPDATE_RECORD];
		max    = subIds.length;
		TransactionalEntity<T> en;
		
		for(int i=0;i<max;i++){
			id = ids[subIds[i]];
			Long pm = pointerMap.get(id);
			en = new TransactionalEntity<T>(id, status, status == TransactionalEntity.DELETE_RECORD? null : entities[i]);
			
			lock.lock();
			try{
				tx.seek(pm);
				tx.write(en);
				tx.flush();
			}
			finally{
				lock.unlock();
			}
			
		}
		
	}

	private T selectEntity(long id) throws IOException{
		
		Long pointer = pointerMap.get(id);
		
		if(pointer == null){
			Lock lock = data.getLock();
			lock.lock();
			try{
				data.seek(id);
				return data.read();
			}
			finally{
				lock.unlock();
			}
		}
		else{
			Lock lock = this.tx.getLock();
			lock.lock();
			try{
				tx.seek(pointer);
				TransactionalEntity<T> e = tx.read();
				return e == null? null : e.getEntity();
			}
			finally{
				lock.unlock();
			}
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
		Lock lock       = this.data.getLock();
		int pos         = 0;
		int q;
		T[] es;
		while(pos < subIds.length){
			
			int nextPos = EntityFileTransactionUtil.getLastSequence(ids, pos);
			q = nextPos - pos;
			
			
			if(q == 1){
				lock.lock();
				try{
					data.seek(subIds[pos]);
					e[pos] = data.read();
				}
				finally{
					lock.unlock();
				}
			}
			else{
				lock.lock();
				try{
					data.seek(subIds[pos]);
					es = data.batchRead(q);
				}
				finally{
					lock.unlock();
				}
				
				System.arraycopy(es, 0, e, pos, q);
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
		lock      = this.tx.getLock();
		pos       = 0;
		TransactionalEntity<T>[] rs;
		
		while(pos < subIds.length){
			
			int nextPos = EntityFileTransactionUtil.getLastSequence(ids, pos);
			q = nextPos - pos;
			
			if(q == 1){
				TransactionalEntity<T> r = tx.read();
				if(r != null){
					lock.lock();
					try{
						tx.seek(subIds[pos]);
						e[pos] = r.getEntity();
					}
					finally{
						lock.unlock();
					}
				}
			}
			else{
				lock.lock();
				try{
					tx.seek(subIds[pos]);
					rs = tx.batchRead(q);
				}
				finally{
					lock.unlock();
				}
				
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

	public TransactionEntityFileAccess<T,R,H> getTransactionEntityFileAccess(){
		return tx;
	}
	
	public EntityFileAccess<T,R,H> getEntityFileAccess(){
		return data;
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
	
}
