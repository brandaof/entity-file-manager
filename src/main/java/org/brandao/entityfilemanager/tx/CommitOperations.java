package org.brandao.entityfilemanager.tx;

import java.io.IOException;

import org.brandao.entityfilemanager.EntityFileAccess;

public class CommitOperations {

	public static <T,R> void insert(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R> data) throws IOException{
	}

	public static <T,R> void update(
			RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R> data) throws IOException{
	}

	public static <T,R> void delete(RawTransactionEntity<R>[] ops, 
			EntityFileAccess<T,R> data) throws IOException{
	}
	
}
