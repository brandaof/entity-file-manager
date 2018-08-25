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

	public void managerPointer(long id) throws IOException{
		
		if(!this.pointers.contains(id)){
			this.lockProvider.tryLock(this.data, id, this.timeout, TimeUnit.SECONDS);
			this.pointers.add(id);
		}
		
	}
	
	public void managerPointer(long[] id) throws IOException{
		
		for(int i=0;i<id.length;i++){
			
			long r = id[i];
			
			if(!this.pointers.contains(r)){
				this.lockProvider.tryLock(this.data, this.timeout, TimeUnit.SECONDS);
				this.pointers.add(r);
			}
			
		}
		
	}
	
	public void release(){
		for(Long p: this.pointers){
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
