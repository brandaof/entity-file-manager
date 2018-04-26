package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.LockProvider;

public class PointerManager<T,R> {

	private Set<Long> pointers;
	
	private TransactionEntityFileAccess<T,R> tx;
	
	private EntityFileAccess<T,R> data;
	
	private LockProvider lockProvider;
	
	public PointerManager(
			TransactionEntityFileAccess<T, R> tx, EntityFileAccess<T, R> data,
			LockProvider lockProvider) {
		this.pointers     = new HashSet<Long>();
		this.tx           = tx;
		this.data         = data;
		this.lockProvider = lockProvider;
	}

	public void managerPointer(long id, boolean insert) throws IOException{
		if(!this.pointers.contains(id)){
			this.lockProvider.lock(this.data, id);
			this.pointers.add(id);
			
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
	public void managerPointer(long[] id, boolean insert) throws IOException{
		
		int[] indexNotManaged = new int[id.length];
		int idxNotManaged     = 0;
		
		for(int i=0;i<id.length;i++){
			
			long r = id[i];
			
			if(!this.pointers.contains(r)){
				indexNotManaged[idxNotManaged++] = i;
				this.lockProvider.lock(this.data, r);
				this.pointers.add(r);
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
