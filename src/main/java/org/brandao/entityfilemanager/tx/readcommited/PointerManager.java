package org.brandao.entityfilemanager.tx.readcommited;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.tx.EmptyLockProvider;
import org.brandao.entityfilemanager.tx.TransactionEntityFileAccess;
import org.brandao.entityfilemanager.tx.TransactionException;

public class PointerManager<T,R,H> {

	private Set<Long> pointers;
	
	private EntityFileAccess<T,R,H> data;
	
	private LockProvider lockProvider;

	private long timeout;
	
	public PointerManager(
			TransactionEntityFileAccess<T, R, H> tx,
			LockProvider lockProvider, long timeout) {
		this.pointers     = new HashSet<Long>();
		this.data         = tx.getEntityFileAccess();
		this.lockProvider = lockProvider;
		this.timeout      = timeout;
	}

	public void managerPointer(long id) throws IOException, TransactionException{
		
		if(!pointers.contains(id)){
			if(!lockProvider.tryLock(this.data, id, this.timeout, TimeUnit.SECONDS)){
				throw new TransactionException("failed to acquire registry lock: " + id);
			}
				
			pointers.add(id);
		}
		
	}
	
	public void managerPointer(long[] id) throws IOException, TransactionException{
		
		for(int i=0;i<id.length;i++){
			
			long r = id[i];
			
			if(!pointers.contains(r)){
				if(!lockProvider.tryLock(this.data, r, this.timeout, TimeUnit.SECONDS)){
					throw new TransactionException("failed to acquire registry lock: " + r);
				}
				pointers.add(r);
			}
			
		}
		
	}
	
	public void release(){
		for(Long p: pointers){
			this.lockProvider.unlock(this.data, p);
		}
	}
	
	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.writeObject(pointers);
		stream.writeObject(data);
		stream.writeLong(timeout);
    }

    @SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
		pointers     = (Set<Long>) stream.readObject();
		data         = (EntityFileAccess<T, R, H>) stream.readObject();
		timeout      = stream.readLong();
		lockProvider = new EmptyLockProvider();
    }
	
}
