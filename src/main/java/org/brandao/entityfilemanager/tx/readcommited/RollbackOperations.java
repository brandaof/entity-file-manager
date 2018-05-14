package org.brandao.entityfilemanager.tx.readcommited;

import java.io.IOException;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.tx.RawTransactionEntity;

public class RollbackOperations {

	public static <T,R> void insert(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R> data) throws IOException{
		
		for(RawTransactionEntity<R> op: ops){
			data.seek(op.getRecordID());
			data.writeRawEntity(op.getEntity());
		}
		
	}

	public static <T,R> void update(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R> data) throws IOException{
		
		for(RawTransactionEntity<R> op: ops){
			data.seek(op.getRecordID());
			data.writeRawEntity(op.getEntity());
		}
		
	}

	public static <T,R> void delete(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R> data) throws IOException{
		
		for(RawTransactionEntity<R> op: ops){
			data.seek(op.getRecordID());
			data.write(null);
		}
		
	}
	
}