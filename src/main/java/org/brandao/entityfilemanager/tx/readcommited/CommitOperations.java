package org.brandao.entityfilemanager.tx.readcommited;

import java.io.IOException;
import java.lang.reflect.Array;

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
			data.seek(ops[0].getRecordID());
			data.writeRaw(ops[0].getEntity());
			return;
		}
		
		long[] ids = new long[ops.length];
		
		for(int i=0;i<ops.length;i++){
			ids[i] = ops[i].getRecordID();
		}
		
		int off = 0;
		int q;
		
		while(off < ids.length){
			int nextOff = EntityFileTransactionUtil.getLastSequence(ids, off);
			q = nextOff - off;
			
			data.seek(ids[off]);
			
			if(q == 1){
				data.writeRaw(ops[off].getEntity());
			}
			else{
				R[] subEntities = (R[]) Array.newInstance(data.getRawType(), q);
	
				for(int i=0;i<q;i++){
					subEntities[i] = ops[off + i].getEntity();
				}
				
				data.seek(ids[off]);
				data.batchWriteRaw(subEntities);
			}
			
			off = nextOff;
		}
	}

	public static <T,R,H> void delete(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R,H> data) throws IOException{
		update(ops, data);
	}
	
}
