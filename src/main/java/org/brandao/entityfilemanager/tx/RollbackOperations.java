package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.util.Set;

import org.brandao.entityfilemanager.EntityFileAccess;

public class RollbackOperations {

	public <T,R> void insert(
			RawTransactionEntity<R> op, EntityFileAccess<T,R> data, 
			EntityFileAccess<Long,byte[]> freeSpace, Set<Long> mappedFreeSpace) throws IOException{
		
		if(!mappedFreeSpace.contains(op.getRecordID())){
			freeSpace.seek(freeSpace.length());
			freeSpace.write(op.getRecordID());
		}
		
	}

	public <T,R> void update(
			RawTransactionEntity<R> op, EntityFileAccess<T,R> data) throws IOException{
		data.seek(op.getRecordID());
		data.writeRawEntity(op.getEntity());
	}
	
}
