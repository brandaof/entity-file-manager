package org.brandao.entityfilemanager.tx.async;

import java.io.File;
import java.io.IOException;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.tx.Await;
import org.brandao.entityfilemanager.tx.TransientTransactionEntityFileAccess;

public class AwaitTransactionEntityFileAccess<T, R, H>
	extends TransientTransactionEntityFileAccess<T, R, H>{

	private Await await;
	
	public AwaitTransactionEntityFileAccess(EntityFileAccess<T, R, H> e,
			File file, long transactionID, byte transactionIsolation, Await await) {
		super(e, file, transactionID, transactionIsolation);
		this.await = await;
		this.await.await();
	}
	
	public void close() throws IOException{
		try{
			super.close();
		}
		finally{
			await.release();
		}
	}

}
