package org.brandao.entityfilemanager.tx.readcommited;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.concurrent.locks.Lock;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil;
import org.brandao.entityfilemanager.tx.RawTransactionEntity;

public class CommitOperations {

	public static <T,R,H> void insert(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R, H> data) throws IOException{
		update(ops, data);
	}

	@SuppressWarnings("unchecked")
	public static <T,R,H> void update(
			RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R,H> data) throws IOException{
		
		if(ops.length == 0){
			return;
		}

		if(ops.length == 1){
			write(data, ops[0].getRecordID(), ops[0].getEntity());
			return;
		}
		
		long[] ids = new long[ops.length];
		
		for(int i=0;i<ops.length;i++){
			ids[i] = ops[i].getRecordID();
		}
		
		int off = 0;
		int nextOff;
		int q;
		
		while(off < ids.length){
			q = EntityFileTransactionUtil.getLenNextSequenceGroup(ids, off);
			nextOff = off + q;
			
			if(q == 1){
				write(data, ids[off], ops[off].getEntity());
			}
			else{
				R[] subEntities = (R[]) Array.newInstance(data.getRawType(), q);
	
				for(int i=0;i<q;i++){
					subEntities[i] = ops[off + i].getEntity();
				}
				
				write(data, ids[off], subEntities);
			}
			
			off = nextOff;
		}
		
	}

	private static <T,R,H> void write(EntityFileAccess<T,R,H> data, 
			long id, R raw) throws IOException{
		Lock lock = data.getLock();
		lock.lock();
		try{
			data.seek(id);
			data.writeRaw(raw);
			data.flush();
		}
		finally{
			lock.unlock();
		}
	}
	
	private static <T,R,H> void write(EntityFileAccess<T,R,H> data, 
			long firstID, R[] raw) throws IOException{
		Lock lock = data.getLock();
		lock.lock();
		try{
			data.seek(firstID);
			data.batchWriteRaw(raw);
			data.flush();
		}
		finally{
			lock.unlock();
		}
	}
	
	public static <T,R,H> void delete(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R,H> data) throws IOException{
		update(ops, data);
	}
	
}
