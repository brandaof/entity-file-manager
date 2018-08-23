package org.brandao.entityfilemanager.tx.readcommited;

import java.io.IOException;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.tx.RawTransactionEntity;

public class RollbackOperations {

	public static <T,R,H> void insert(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R,H> data) throws IOException{
	}

	public static <T,R,H> void update(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R,H> data) throws IOException{
	}

	public static <T,R,H> void delete(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R,H> data) throws IOException{
	}
	
}
